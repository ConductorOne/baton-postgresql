package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/cli"
)

// config defines the external configuration required for the connector to run.
type config struct {
	cli.BaseConfig `mapstructure:",squash"` // Puts the base config options in the same place as the connector options

	Dsn            string   `mapstructure:"dsn"`
	Schemas        []string `mapstructure:"schemas"`
	IncludeColumns bool     `mapstructure:"include-columns"`
}

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(ctx context.Context, cfg *config) error {
	if cfg.Dsn == "" {
		return fmt.Errorf("--dsn is required")
	}

	return nil
}
