package janetk8sreceiver

import "go.opentelemetry.io/collector/component"

type Config struct {
	// Path to kubeconfig. Empty means in-cluster config.
	KubeconfigPath string `mapstructure:"kubeconfig_path"`
}

func (c *Config) Validate() error {
	return nil
}

var _ component.Config = (*Config)(nil)
