package main

import (
	"context"

	cfg "github.com/conductorone/baton-postgresql/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-postgresql/pkg/connector"
	configschema "github.com/conductorone/baton-sdk/pkg/config"
)

var version = "dev"

func main() {
	ctx := context.Background()

	// The capabilities subcommand runs without a DSN, so it builds the
	// zero-value connector instead of validating the required flag.
	configschema.RunConnector(ctx, "baton-postgresql", version, cfg.Config, getConnector,
		connectorrunner.WithDefaultCapabilitiesConnectorBuilderV2(&connector.Postgresql{}))
}

func getConnector(ctx context.Context, pgc *cfg.Postgresql, _ *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	l := ctxzap.Extract(ctx)

	cb, err := connector.New(ctx, pgc.Dsn, pgc.Schemas, pgc.IncludeColumns, pgc.IncludeLargeObjects, pgc.SyncAllDatabases, pgc.SkipBuiltInFunctions)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, nil, err
	}

	return cb, nil, nil
}
