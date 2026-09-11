package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const withGrantOptions = " WITH GRANT OPTION"

type ClientDatabasesPool struct {
	databases        map[string]*Client
	opts             []ClientOpt
	mutex            *sync.Mutex
	logger           *Logger
	dsn              string
	defaultClientDsn *Client
}

func NewClientDatabasesPool(ctx context.Context, dsn string, opts ...ClientOpt) (*ClientDatabasesPool, error) {
	l := ctxzap.Extract(ctx)

	defaultClientDsn, err := New(ctx, dsn, opts...)
	if err != nil {
		l.Error("failed to create default database client", zap.Error(err))
		return nil, err
	}

	return &ClientDatabasesPool{
		dsn:              dsn,
		databases:        make(map[string]*Client),
		opts:             opts,
		mutex:            &sync.Mutex{},
		logger:           &Logger{},
		defaultClientDsn: defaultClientDsn,
	}, nil
}

func (p *ClientDatabasesPool) Default(ctx context.Context) *Client {
	// A nil pool is the zero-value connector behind the capabilities
	// subcommand: its syncers declare themselves and never query.
	if p == nil {
		return nil
	}
	return p.defaultClientDsn
}

func (p *ClientDatabasesPool) Get(ctx context.Context, database string) (*Client, string, error) {
	l := ctxzap.Extract(ctx)

	dbModel, err := p.defaultClientDsn.GetDatabaseById(ctx, database)
	if err != nil {
		return nil, "", err
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if client, ok := p.databases[dbModel.Name]; ok {
		err := client.ValidateConnection(ctx)

		if err != nil {
			l.Error("database connection is invalid", zap.String("database", dbModel.Name), zap.Error(err))
			client.db.Close()
			delete(p.databases, dbModel.Name)
		} else {
			return client, dbModel.Name, nil
		}
	}

	config, err := pgxpool.ParseConfig(p.dsn)
	if err != nil {
		return nil, "", err
	}

	logger := &Logger{}
	config.ConnConfig.LogLevel = logger.Zap2PgxLogLevel(l.Level())
	config.ConnConfig.Logger = logger
	config.ConnConfig.Database = dbModel.Name

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, "", err
	}

	c := &Client{
		db:  db,
		cfg: config,
	}

	for _, o := range p.opts {
		o(c)
	}

	p.databases[dbModel.Name] = c

	return c, dbModel.Name, nil
}

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

// classifyConnectError attaches a gRPC status to connection failures the
// server attributes to the client's authorization, so the process exit code
// reports a configuration problem instead of an unknown error. SQLSTATE
// class 28 is "invalid authorization specification" (bad user or password,
// or no matching pg_hba.conf entry); 42501 is "insufficient privilege" (no
// CONNECT on the database). The original error stays in the chain.
func classifyConnectError(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch {
	case strings.HasPrefix(pgErr.Code, "28"):
		return fmt.Errorf("%w: %w", status.Error(codes.Unauthenticated, "postgres authentication failed"), err)
	case pgErr.Code == "42501":
		return fmt.Errorf("%w: %w", status.Error(codes.PermissionDenied, "postgres permission denied"), err)
	}
	return err
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

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, classifyConnectError(err)
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

func (c *Client) DatabaseName() string {
	return c.cfg.ConnConfig.Database
}
