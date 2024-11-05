package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type KafkaConfig struct {
	Brokers                 []string `yaml:"brokers"`
	Username                string   `yaml:"username"`
	Password                string   `yaml:"password"`
	Topic                   string   `yaml:"topic"`
	Retries                 int      `yaml:"retries"`
	ProducerReturnSuccesses bool     `yaml:"producer_return_successes"`
}

type LogConfig struct {
	RotationSize  int `yaml:"rotation_size"`
	RotationCount int `yaml:"rotation_count"`
}

type Config struct {
	Kafka KafkaConfig `yaml:"kafka"`
	Log   LogConfig   `yaml:"log"`
}

func LoadConfig(configPath string) (*Config, error) {
	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		log.Fatalf("Config file does not exist: %v", err)
	}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
