package main

import (
	"context"
	"fmt"
	"kafka-example/config"
	"kafka-example/constant"
	"kafka-example/kafka"
	"kafka-example/logger"
	"math/rand"
	"time"

	"go.uber.org/zap"
)

func startProducer(ctx context.Context, cfg *config.Config, log *zap.Logger) error {

	producer, err := kafka.NewProducer(&cfg.Kafka, cfg.Kafka.Topic, log)
	if err != nil {
		return err
	}
	defer producer.Close()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Send Messages: A loop that sends messages to Kafka every 3 seconds.
	for counter := 1; ; counter++ {
		<-ticker.C

		producer.SendMessage(ctx, fmt.Sprintf("msg-key-%d", counter), fmt.Sprintf("Counter message %d", counter))

	}
}

func main() {
	cfg, _ := config.LoadConfig("config/config.yaml")
	log := logger.NewLogger("producer", cfg.Log.RotationSize, cfg.Log.RotationCount)

	ctx := context.Background()

	opID := fmt.Sprintf("op-%d", rand.Intn(1000))

	ctx = context.WithValue(ctx, constant.OperationID, opID)
	ctx = context.WithValue(ctx, constant.OpUserID, "user-396")

	if err := startProducer(ctx, cfg, log); err != nil {
		log.Fatal("Failed to start producer", zap.Error(err))
	}
}
