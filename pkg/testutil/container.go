package testutil

import (
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

//go:embed init.sql
var initScript string

type SQLContainer struct {
	sqlDB     *pgxpool.Pool
	container *postgres.PostgresContainer
	dsn       string
}

func (d *SQLContainer) Dsn() string {
	return d.dsn
}

func (d *SQLContainer) Db() *pgxpool.Pool {
	return d.sqlDB
}

func (d *SQLContainer) Container() *postgres.PostgresContainer {
	return d.container
}

func (d *SQLContainer) Role() string {
	return "test_role"
}

func SetupPostgresContainer(ctx context.Context, t *testing.T) *SQLContainer {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)

	assert.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	t.Log("Postgres connection: " + connStr)

	config, err := pgxpool.ParseConfig(connStr)
	assert.NoError(t, err)

	config.ConnConfig.LogLevel = pgx.LogLevelDebug
	config.ConnConfig.Logger = pgx.LoggerFunc(func(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
		t.Logf("PGX %s: %s - %v", level.String(), msg, data)
	})
	config.MaxConns = 2

	db, err := pgxpool.ConnectConfig(ctx, config)
	assert.NoError(t, err)

	_, err = db.Exec(ctx, initScript)
	assert.NoError(t, err)

	return &SQLContainer{
		sqlDB:     db,
		container: pgContainer,
		dsn:       connStr,
	}
}
