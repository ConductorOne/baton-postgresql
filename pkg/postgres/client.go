package postgres

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Client struct {
	db  *pgxpool.Pool
	cfg *pgxpool.Config
}

func (c *Client) ValidateConnection(ctx context.Context) error {
	err := c.db.Ping(ctx)
	if err != nil {
		return err
	}

	return nil
}

func New(ctx context.Context, dsn string) (*Client, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	spew.Dump(config)

	if config.ConnConfig.Database == "" {
		return nil, fmt.Errorf("must specify a database to connect to")
	}

	db, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}

	c := &Client{
		db:  db,
		cfg: config,
	}

	return c, nil
}
