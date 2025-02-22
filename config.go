package kafkathena_gox

import (
	"fmt"

	"github.com/spf13/viper"
)

// RootConfig: Tüm yapılandırmanın en üst seviyedeki temsilidir.
type RootConfig struct {
	KafkaAthena KafkaAthenaConfig `mapstructure:"kafkathena"`
}

// KafkaAthenaConfig: kafkathena anahtarının altındaki yapılandırmaları içerir.
type KafkaAthenaConfig struct {
	SharedFactoryProps SharedFactoryProps            `mapstructure:"shared-factory-props"`
	Producers          map[string]ProducerConfig     `mapstructure:"producers"`
	Consumers          map[string]ConsumerConfig     `mapstructure:"consumers"`
	IntegrationTopics  map[string][]IntegrationTopic `mapstructure:"integration-topics"`
}

// SharedFactoryProps: Hem producer hem de consumer için ortak ayarları içerir.
type SharedFactoryProps struct {
	Producer SharedProducerProps `mapstructure:"producer"`
	Consumer SharedConsumerProps `mapstructure:"consumer"`
}

// SharedProducerProps: Producer ile ilgili ortak ayarlar.
type SharedProducerProps struct {
	Interceptor string `mapstructure:"interceptor"`
}

// SharedConsumerProps: Consumer ile ilgili ortak ayarlar.
type SharedConsumerProps struct {
	AutoStartup             bool                     `mapstructure:"autoStartup"`
	MissingTopicAlertEnable bool                     `mapstructure:"missingTopicAlertEnable"`
	Concurrency             int                      `mapstructure:"concurrency"`
	SyncCommitTimeoutSecond int                      `mapstructure:"syncCommitTimeoutSecond"`
	SyncCommit              bool                     `mapstructure:"syncCommit"`
	AckMode                 string                   `mapstructure:"ackMode"`
	IsBatch                 bool                     `mapstructure:"isBatch"`
	Interceptor             string                   `mapstructure:"interceptor"`
	Clusters                map[string]ClusterConfig `mapstructure:"clusters"`
}

// ClusterConfig: Her bir cluster için sunucu ve ek özellikler.
type ClusterConfig struct {
	Servers         string                 `mapstructure:"servers"`
	AdditionalProps map[string]interface{} `mapstructure:"additional-props"`
}

// ProducerConfig: Producer yapılandırmasını temsil eder.
type ProducerConfig struct {
	Cluster  string                 `mapstructure:"cluster"`
	ClientID string                 `mapstructure:"client-id"`
	Props    map[string]interface{} `mapstructure:"props"`
}

// ConsumerConfig: Consumer yapılandırmasını temsil eder.
type ConsumerConfig struct {
	Type                    string                 `mapstructure:"type"`
	Topic                   string                 `mapstructure:"topic"`
	FactoryBeanName         string                 `mapstructure:"factory-bean-name"`
	DataClass               string                 `mapstructure:"data-class"`
	ErrorProducerName       string                 `mapstructure:"error-producer-name"`
	Cluster                 string                 `mapstructure:"cluster"`
	ClientID                string                 `mapstructure:"client-id"`
	FilterHeader            FilterHeaderConfig     `mapstructure:"filter-header"`
	IgnoredExceptionClasses []string               `mapstructure:"ignored-exception-classes"`
	Failover                FailoverConfig         `mapstructure:"failover"`
	FixedRetry              RetryConfig            `mapstructure:"fixed-retry"`
	ExponentialRetry        ExponentialRetryConfig `mapstructure:"exponential-retry"`
	FactoryProps            map[string]interface{} `mapstructure:"factory-props"`
	Props                   map[string]interface{} `mapstructure:"props"`
}

// FilterHeaderConfig: Consumer için header filtreleme ayarları.
type FilterHeaderConfig struct {
	ErrorProducerFilterKey string           `mapstructure:"error-producer-filter-key"`
	ConsumerFilterKey      string           `mapstructure:"consumer-filter-key"`
	CustomHeaders          []HeaderKeyValue `mapstructure:"custom-headers"`
}

// HeaderKeyValue: Header içerisindeki key-value çiftlerini temsil eder.
type HeaderKeyValue struct {
	Key   string `mapstructure:"key"`
	Value string `mapstructure:"value"`
}

// FailoverConfig: Hata durumunda uygulanacak failover ayarları.
type FailoverConfig struct {
	ErrorTopic      string `mapstructure:"error-topic"`
	HandlerBeanName string `mapstructure:"handler-bean-name"`
}

// RetryConfig: Fixed retry ayarlarını temsil eder.
type RetryConfig struct {
	RetryCount            int `mapstructure:"retry-count"`
	BackoffIntervalMillis int `mapstructure:"backoff-interval-millis"`
}

// ExponentialRetryConfig: Üssel retry ayarlarını temsil eder.
type ExponentialRetryConfig struct {
	RetryCount            int `mapstructure:"retry-count"`
	Multiplier            int `mapstructure:"multiplier"`
	MaxInterval           int `mapstructure:"maxInterval"`
	BackoffIntervalMillis int `mapstructure:"backoff-interval-millis"`
}

// IntegrationTopic: Entegrasyon topic bilgilerini temsil eder.
type IntegrationTopic struct {
	Name          string   `mapstructure:"name"`
	ProducerNames []string `mapstructure:"producer-names"`
}

// LoadConfig, "application.yaml" (ya da başka format) dosyasını mevcut çalışma dizininde arar,
// konfigürasyonu yükler ve RootConfig tipinde döner.
func LoadConfig() (*RootConfig, error) {
	v := viper.New()
	v.SetConfigName("application") // dosya adı: application.yaml (uzantısız)
	v.AddConfigPath(".")           // geçerli dizinde arar
	v.AutomaticEnv()               // environment değişkenlerini de kullanır

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("konfigürasyon dosyası okunamadı: %w", err)
	}

	var config RootConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("konfigürasyon ayrıştırılırken hata: %w", err)
	}

	return &config, nil
}
