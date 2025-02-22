package kafkathena_gox

import (
	"context"
	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	"os"
)

type Consumer interface {
	Subscribe(handler EventHandler)
	Unsubscribe()
}

type EventHandler interface {
	Setup() error
	Cleanup() error
	HandleMessage(msg *ck.Message) error
}

// kafkaConsumer, confluent-kafka-go kullanarak oluşturulmuş tüketici yapısını temsil eder.
type kafkaConsumer struct {
	topics     []string
	errorTopic string
	consumer   *ck.Consumer
}

// NewConsumer, verilen bağlantı parametrelerine göre confluent-kafka-go Consumer oluşturur.
func NewConsumer(sharedConsumerProps *SharedConsumerProps, consumerConfig *ConsumerConfig) (Consumer, error) {
	// Brokers listesini virgülle ayrılmış şekilde alıyoruz.
	brokers := sharedConsumerProps.Clusters[consumerConfig.Cluster]

	configMap := &ck.ConfigMap{
		"bootstrap.servers": brokers,
	}
	for k, v := range consumerConfig.Props {
		err := configMap.SetKey(k, v)
		if err != nil {
			//ignore
		}
	}

	consumer, err := ck.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}

	zap.S().Infof("Kafka consumer listens group: %s", consumerConfig.Props["group.id"])
	return &kafkaConsumer{
		topics:     []string{consumerConfig.Topic},
		errorTopic: consumerConfig.Failover.ErrorTopic,
		consumer:   consumer,
	}, nil
}

// Subscribe, consumer'ı belirtilen topic(ler)e abone eder ve mesajları handler üzerinden işleme alır.
func (c *kafkaConsumer) Subscribe(handler EventHandler) {
	ctx := context.Background()
	// Abone olunacak topic listesi: eğer errorTopic tanımlıysa listeye ekleniyor.
	topics := func() []string {
		result := make([]string, 0)
		if c.errorTopic != "" {
			result = append(result, c.errorTopic)
		}
		result = append(result, c.topics...)
		return result
	}()

	// Topic'lere abone ol
	if err := c.consumer.SubscribeTopics(topics, nil); err != nil {
		zap.S().Errorf("Failed to subscribe to topics: %v", err)
		os.Exit(1)
		return
	}

	// Handler için setup çağrısı (opsiyonel)
	if err := handler.Setup(); err != nil {
		zap.S().Errorf("Handler setup error: %v", err)
	}

	// Mesaj tüketimi için goroutine
	go func() {
		for {
			// Poll ile event dinleniyor (100ms timeout)
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *ck.Message:
				// Mesaj geldiğinde handler ile işleniyor
				if err := handler.HandleMessage(e); err != nil {
					zap.S().Errorf("Error handling message: %v", err)
					// Hata durumunda error topic'e gönderme veya custom hata yönetimi eklenebilir.
					if c.errorTopic != "" {
						zap.S().Errorf("Hata mesajı için error topic'e gönderim yapılamadı; implementasyon eklenmeli.")
					}
				}
			case ck.Error:
				// Kafka hata eventleri
				zap.S().Errorf("Kafka error: %v", e)
			default:
				// Diğer eventler
				zap.S().Infof("Ignored event: %v", e)
			}

			if ctx.Err() != nil {
				zap.S().Errorf("Context error: %v", ctx.Err())
				return
			}
		}
	}()

	// Ek hata eventleri dinleyicisi (opsiyonel)
	go func() {
		// confluent-kafka-go'nun Events() kanalını dinlemek de mümkün;
		// örneğin: for ev := range c.consumer.Events() { ... }
		// Burada sadece poll'da alınan hatalar loglanıyor.
	}()
	zap.S().Infof("Kafka consumer listens topics: %v", c.topics)
}

// Unsubscribe, consumer'ı kapatarak aboneliği sonlandırır.
func (c *kafkaConsumer) Unsubscribe() {
	if err := c.consumer.Close(); err != nil {
		zap.S().Errorf("Consumer wasn't closed: %v", err)
	}
	zap.S().Info("Kafka consumer closed")
}
