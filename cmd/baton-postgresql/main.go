package main

import (
	"context"

	cfg "github.com/conductorone/baton-postgresql/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"

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
	// RunConnector prints the returned error on exit, so don't log it here.
	cb, err := connector.New(ctx, pgc.Dsn, pgc.Schemas, pgc.IncludeColumns, pgc.IncludeLargeObjects, pgc.SyncAllDatabases, pgc.SkipBuiltInFunctions)
	if err != nil {
		return nil, nil, err
	}

	return cb, nil, nil
}
