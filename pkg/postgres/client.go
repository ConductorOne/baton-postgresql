package postgres

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Client struct {
	db           *pgxpool.Pool
	cfg          *pgxpool.Config
	schemaFilter []string
}

func (c *Client) ValidateConnection(ctx context.Context) error {
	err := c.db.Ping(ctx)
	if err != nil {
		return err
	}

	return nil
}

type ClientOpt func(c *Client)

func WithSchemaFilter(filter []string) ClientOpt {
	return func(c *Client) {
		c.schemaFilter = filter
	}
}

func New(ctx context.Context, dsn string, opts ...ClientOpt) (*Client, error) {
	l := ctxzap.Extract(ctx)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	logger := &Logger{}
	config.ConnConfig.LogLevel = logger.Zap2PgxLogLevel(l.Level())
	config.ConnConfig.Logger = logger

	if config.ConnConfig.Database == "" {
		return nil, fmt.Errorf("must specify a database to connect to")
	}

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	c := &Client{
		db:  db,
		cfg: config,
	}

	for _, o := range opts {
		o(c)
	}

	return c, nil
}
