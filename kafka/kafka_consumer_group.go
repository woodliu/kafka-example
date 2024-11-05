package kafka

import (
	"context"
	"kafka-example/config"
	"kafka-example/constant"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type MConsumerGroup struct {
	config *config.KafkaConfig
	topic  string
	group  sarama.ConsumerGroup
	logger *zap.Logger
}

func NewConsumerGroup(cfg *config.KafkaConfig, topic string, groupId string, consumerId string, logger *zap.Logger) (*MConsumerGroup, error) {
	saramaConfig := sarama.NewConfig()

	// OffsetOldest stands for the oldest offset available on the broker for a partition.
	// We can send this to a client's GetOffset method to get this offset, or when calling ConsumePartition to start consuming from the oldest offset that is still available on the broker.
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	// If enabled, any errors that occurred while consuming are returned on the Errors channel (default disabled).
	saramaConfig.Consumer.Return.Errors = true

	// Setting saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()} specifies how Kafka partitions are assigned to consumers within a consumer group.
	// The Range strategy (NewBalanceStrategyRange) divides partitions among consumers by assigning consecutive partitions to each consumer.
	// This ensures a balanced distribution of partitions, especially when the number of partitions is divisible by the number of consumers. This strategy is often used to maintain a predictable partition assignment.
	// Alternative strategies, like RoundRobin, distribute partitions more evenly in cases with mismatched partition-consumer counts.
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	if cfg.Username != "" && cfg.Password != "" {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.Username
		saramaConfig.Net.SASL.Password = cfg.Password
	}

	group, err := sarama.NewConsumerGroup(cfg.Brokers, groupId, saramaConfig)
	if err != nil {
		logger.Error("Failed to create consumer group", zap.Error(err))
		return nil, err
	}

	logger.Info("Success to create or connect to existed consumerGroup", zap.String("consumerID", consumerId))

	// Handle errors in consumer group
	go func() {
		// Handle Errors: Listen for errors in the consumer group by checking the Errors() method on the consumer group session, which provides error events.
		for err := range group.Errors() {
			logger.Error("Consumer group error", zap.Error(err))
		}
	}()

	return &MConsumerGroup{config: cfg, topic: topic, group: group, logger: logger}, nil
}

func (mc *MConsumerGroup) GetContextFromMsg(cMsg *sarama.ConsumerMessage) context.Context {
	var values []string
	for _, recordHeader := range cMsg.Headers {
		values = append(values, string(recordHeader.Value))
	}
	mapper := []constant.ContextKey{constant.OperationID, constant.OpUserID}
	ctx := context.Background()
	for i, value := range values {
		ctx = context.WithValue(ctx, mapper[i], value)
	}
	return ctx
}

func (c *MConsumerGroup) RegisterHandlerAndConsumeMessages(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	defer c.group.Close()
	for {
		if err := c.group.Consume(ctx, []string{c.topic}, handler); err != nil {
			c.logger.Error("Error consuming messages", zap.Error(err))
			time.Sleep(2 * time.Second) // retry delay
		}
	}
}

func (c *MConsumerGroup) Close() error {
	return c.group.Close()
}
