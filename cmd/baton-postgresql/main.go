package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-postgresql/pkg/connector"
)

var version = "dev"

func main() {
	ctx := context.Background()

	cfg := &config{}
	cmd, err := cli.NewCmd(ctx, "baton-postgresql", cfg, validateConfig, getConnector)
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

	cmd.PersistentFlags().Bool(
		"include-columns",
		false,
		"Include column privileges when syncing. This can result in large amounts of data. ($BATON_INCLUDE_COLUMNS)\nThis defaults to false.",
	)

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func getConnector(ctx context.Context, cfg *config) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	cb, err := connector.New(ctx, cfg.Dsn, cfg.Schemas, cfg.IncludeColumns)
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
