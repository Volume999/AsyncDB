package config

import (
	"fmt"
	"github.com/spf13/viper"
	"strings"
)

type AppConfig struct {
	OrderServiceImplementation string `mapstructure:"order_service_implementation"`
}

func LoadConfig(configPath string) (*AppConfig, error) {
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(configPath)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file, %s", err)
	}

	var appConfig AppConfig
	if err := viper.Unmarshal(&appConfig); err != nil {
		return nil, fmt.Errorf("unable to decode into struct, %v", err)
	}
	return &appConfig, nil
}
