package main

import (
	"context"
	"fmt"
	"os"

	cfg "github.com/conductorone/baton-postgresql/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-postgresql/pkg/connector"
	configschema "github.com/conductorone/baton-sdk/pkg/config"
)

var version = "dev"

func main() {
	ctx := context.Background()

	_, cmd, err := configschema.DefineConfiguration(ctx, "baton-postgresql", getConnector, cfg.Config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func getConnector(ctx context.Context, pgc *cfg.Postgresql) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	cb, err := connector.New(ctx, pgc.Dsn, pgc.Schemas, pgc.IncludeColumns, pgc.IncludeLargeObjects)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	newConnector, err := connectorbuilder.NewConnector(ctx, cb)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return newConnector, nil
}
