package janetk8sreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const ReceiverVersion = "0.1.0"

var typeStr = component.MustNewType("janetk8s")

// NewFactory returns the OTel receiver factory for janetk8s.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	rCfg := cfg.(*Config)
	if err := rCfg.Validate(); err != nil {
		return nil, err
	}
	return newReceiver(rCfg, set.Logger, nextConsumer)
}
