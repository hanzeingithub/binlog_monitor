package binlog_monitor

import (
	"github.com/Shopify/sarama"
	"github.com/go-mysql-org/go-mysql/canal"
)

type monitorConfig struct {
	canalConf    *canal.Config
	kafkaConf    *sarama.Config
	clientAddr   []string
	defaultTopic string
}

func initConf() {

}

func getDefaultConf() *monitorConfig {
	kafka := sarama.NewConfig()
	kafka.Producer.RequiredAcks = sarama.WaitForAll
	kafka.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	kafka.Producer.Return.Successes = true
	return &monitorConfig{
		canalConf:  canal.NewDefaultConfig(),
		kafkaConf:  kafka,
		clientAddr: []string{},
	}
}
