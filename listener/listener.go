package listener

import (
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// In case work with asyncProducer
func ListenAsyncProducerStatus(producer sarama.AsyncProducer, log *zap.Logger) {
	go func() {
		for err := range producer.Errors() {
			// Convert sarama.Encoder to []byte, then to string
			valueBytes, _ := err.Msg.Value.Encode()
			log.Error("Producer error", zap.Error(err.Err), zap.String("msg", string(valueBytes)))
		}
	}()

	go func() {
		for msg := range producer.Successes() {
			log.Info("Message acknowledged", zap.String("topic", msg.Topic), zap.Int32("partition", msg.Partition), zap.Int64("offset", msg.Offset))
		}
	}()
}
