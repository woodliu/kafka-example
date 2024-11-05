package kafka

import (
	"context"
	"errors"
	"fmt"
	"kafka-example/config"
	"kafka-example/constant"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type MProducer struct {
	// producer sarama.AsyncProducer
	producer sarama.SyncProducer
	topic    string
	logger   *zap.Logger
	config   *config.KafkaConfig
}

func NewProducer(cfg *config.KafkaConfig, topic string, log *zap.Logger) (*MProducer, error) {
	saramaConfig := sarama.NewConfig()

	// The total number of times to retry sending a message (default 3)
	// the producer will stop retrying to send the message after 5 failed attempts.
	// This means the message could be dropped if it hasn't successfully been sent after these retries, potentially resulting in message loss unless other safeguards (like error handling or dead-letter queues) are in place.
	saramaConfig.Producer.Retry.Max = 5

	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via the `min.insync.replicas` configuration key.
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll

	// Setting saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner configures the Kafka producer to use a hash-based partitioner for determining which partition a message should go to.
	// This partitioner applies a hash function to the message key, ensuring messages with the same key consistently go to the same partition. This is useful for maintaining ordering for specific keys, as all messages with that key will always be sent to the same partition.
	// When sending a message, we must specify the key value of the message. If there is no key, the partition will be selected randomly
	saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner

	// In sarama.SyncProducer, setting Producer.Return.Successes = true is required to receive message acknowledgments after successful sends.
	// Without this, SyncProducer won’t wait for broker acknowledgment, making it impossible to return partition and offset information for sent messages. Setting this option ensures that SendMessage can confirm successful delivery with metadata, enhancing reliability.
	saramaConfig.Producer.Return.Successes = true

	if cfg.Username != "" && cfg.Password != "" {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.Username
		saramaConfig.Net.SASL.Password = cfg.Password
	}

	// Following only for working with AsyncProducer, where we handle Errors and Succcess asynchronously
	// saramaConfig.Producer.Return.Errors = true
	// saramaConfig.Producer.Return.Successes = cfg.Kafka.ProducerReturnSuccesses
	// ListenAsyncProducerStatus(asyncProcuder,log)

	var prod sarama.SyncProducer
	var err error

	for i := 0; i <= cfg.Retries; i++ {
		// prod, err := sarama.NewAsyncProducer(cfg.Kafka.Brokers, saramaConfig)
		prod, err = sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
		if err == nil {
			break
		} else {
			log.Error("Failed to create producer", zap.Int("tryTime", i), zap.Error(err))
		}

		time.Sleep(time.Duration(1) * time.Second)
	}

	if err != nil {
		log.Error("Failed to create producer after many tries", zap.Error(err))
		return nil, err
	}

	log.Info("Success to create producer")

	// The main differences between sarama.SyncProducer and sarama.AsyncProducer are:
	// Message Delivery Mechanism:
	// SyncProducer: Sends messages synchronously. Each SendMessage call waits for the broker’s acknowledgment, making it blocking and ensuring delivery order.
	// AsyncProducer: Sends messages asynchronously through channels (Input() for messages, Errors() for errors, and optionally Successes() for successful deliveries). It’s non-blocking and faster for high-throughput needs.
	// Use Cases:
	// SyncProducer: Ideal for low-throughput scenarios where message delivery guarantees and ordering are critical.
	// AsyncProducer: Suitable for high-throughput applications where latency is prioritized, and managing message acknowledgment and error handling is feasible.

	return &MProducer{producer: prod, topic: topic, config: cfg, logger: log}, nil
}

// Send context Data between producer consumers via Header
func GetMQHeaderWithContext(ctx context.Context) ([]sarama.RecordHeader, error) {
	operationID, ok := ctx.Value(constant.OperationID).(string)
	if !ok {
		err := errors.New("ctx missing operationID")
		return nil, err
	}
	opUserID, ok := ctx.Value(constant.OpUserID).(string)
	if !ok {
		err := errors.New("ctx missing userID")
		return nil, err
	}
	return []sarama.RecordHeader{
		{Key: []byte(constant.OperationID), Value: []byte(operationID)},
		{Key: []byte(constant.OpUserID), Value: []byte(opUserID)},
	}, nil
}

func (p *MProducer) SendMessage(ctx context.Context, key, msgValue string) error {

	header, err := GetMQHeaderWithContext(ctx)
	if err != nil {
		p.logger.Error("Failed to get Header", zap.Error(err))
	}

	kafkaMsg := &sarama.ProducerMessage{
		Topic:   p.topic,
		Key:     sarama.StringEncoder(key),
		Value:   sarama.StringEncoder(msgValue),
		Headers: header,
	}

	partition, offset, err := p.producer.SendMessage(kafkaMsg)
	if err != nil {
		p.logger.Error("Failed to send message", zap.Error(err))
		return err
	}

	fmt.Println("[Message Sent] ", "topic:", p.topic, " - key:", key, " - msg:", msgValue, " - partition:", partition, " - offset:", offset)

	// Logging message sent
	// p.logger.Info("Message sent",
	//  zap.String("topic", p.topic),
	//  zap.String("key", key),
	//  zap.String("msg", msgValue),
	//  zap.Int32("partition", partition),
	//  zap.Int64("offset", offset),
	// )

	return nil
}

func (p *MProducer) Close() error {
	return p.producer.Close()
}
