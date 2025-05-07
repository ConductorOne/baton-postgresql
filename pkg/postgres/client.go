package postgres

import (
	"context"
	"errors"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

type ClientDatabasesPool struct {
	databases map[string]*Client
	opts      []ClientOpt
	mutex     *sync.Mutex
	logger    *Logger
	dsn       string
	// Only used if dsn have any database name
	defaultClientWithDatabase *Client
	defaultClientDsn          *Client
}

func NewClientDatabasesPool(ctx context.Context, dsn string, opts ...ClientOpt) (*ClientDatabasesPool, error) {
	l := ctxzap.Extract(ctx)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	var defaultClientWithDatabase *Client
	if config.ConnConfig.Database != "" {
		l.Info("using default database database config", zap.String("database", config.ConnConfig.Database))
		defaultClientWithDatabase, err = New(ctx, dsn, opts...)
		if err != nil {
			return nil, err
		}
	}

	defaultClientDsn, err := New(ctx, dsn, opts...)
	if err != nil {
		l.Error("failed to create default database client", zap.Error(err))
		return nil, err
	}

	return &ClientDatabasesPool{
		dsn:                       dsn,
		databases:                 make(map[string]*Client),
		opts:                      opts,
		mutex:                     &sync.Mutex{},
		logger:                    &Logger{},
		defaultClientWithDatabase: defaultClientWithDatabase,
		defaultClientDsn:          defaultClientDsn,
	}, nil
}

func (c *ClientDatabasesPool) DefaultDatabase(ctx context.Context) string {
	if c.defaultClientWithDatabase == nil {
		return ""
	}

	return c.defaultClientWithDatabase.cfg.ConnConfig.Database
}

func (p *ClientDatabasesPool) Default(ctx context.Context) *Client {
	return p.defaultClientDsn
}

func (p *ClientDatabasesPool) Get(ctx context.Context, database string) (*Client, string, error) {
	l := ctxzap.Extract(ctx)

	if database == "" {
		if p.defaultClientWithDatabase != nil {
			dbName := p.defaultClientWithDatabase.db.Config().ConnConfig.Database
			return p.defaultClientWithDatabase, dbName, nil
		}

		return nil, "", errors.New("database name is required")
	}

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
