package testutil

import (
	"context"
	"database/sql"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

//go:embed init.sql
var initScript string

type SQLContainer struct {
	sqlDB     *sql.DB
	container *postgres.PostgresContainer
}

func (d *SQLContainer) Db() *sql.DB {
	return d.sqlDB
}

func SetupPostgresContainer(t *testing.T) *SQLContainer {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)

	assert.NoError(t, err)

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	t.Log("Postgres connection: " + connStr)

	sqlDB, err := sql.Open("pgx", connStr)
	assert.NoError(t, err)

	_, err = sqlDB.Exec(initScript)
	assert.NoError(t, err)

	return &SQLContainer{
		sqlDB:     sqlDB,
		container: pgContainer,
	}
}
