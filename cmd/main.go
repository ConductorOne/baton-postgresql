package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/sdk"

	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-postgresql/pkg/connector"
)

var version = "dev"

func main() {
	ctx := context.Background()

	cfg := &config{}
	cmd, err := cli.NewCmd(ctx, "baton-postgresql", cfg, validateConfig, getConnector, run)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version

	cmd.PersistentFlags().String(
		"dsn",
		"",
		"The connection string for the PostgreSQL database ($BATON_DSN)\nexample: postgres://username:password@localhost:5432/database_name",
	)

	cmd.PersistentFlags().StringSlice(
		"schemas",
		[]string{"public"},
		"The schemas to include in the sync. ($BATON_SCHEMAS)\nThis defaults to 'public' only.",
	)

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func getConnector(ctx context.Context, cfg *config) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	cb, err := connector.New(ctx, cfg.Dsn, cfg.Schemas)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	connector, err := connectorbuilder.NewConnector(ctx, cb)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return connector, nil
}

// run is where the the process of syncing with the connector is implemented.
func run(ctx context.Context, cfg *config) error {
	l := ctxzap.Extract(ctx)

	c, err := getConnector(ctx, cfg)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return err
	}

	r, err := sdk.NewConnectorRunner(ctx, c, cfg.C1zPath)
	if err != nil {
		l.Error("error creating connector runner", zap.Error(err))
		return err
	}
	defer r.Close()

	err = r.Run(ctx)
	if err != nil {
		l.Error("error running connector", zap.Error(err))
		return err
	}

	return nil
}
