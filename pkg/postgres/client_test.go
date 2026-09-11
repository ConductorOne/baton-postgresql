package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientDatabasesPoolGetPermissionDenied(t *testing.T) {
	ctx := context.Background()
	container := testutil.SetupPostgresContainer(ctx, t)
	t.Cleanup(func() {
		container.Db().Close()
		require.NoError(t, container.Container().Terminate(ctx))
	})

	_, err := container.Db().Exec(ctx, "CREATE ROLE limited_login LOGIN PASSWORD 'test-password'")
	require.NoError(t, err)
	_, err = container.Db().Exec(ctx, "CREATE DATABASE restricted_database")
	require.NoError(t, err)
	_, err = container.Db().Exec(ctx, "REVOKE CONNECT ON DATABASE restricted_database FROM PUBLIC")
	require.NoError(t, err)

	dsn, err := url.Parse(container.Dsn())
	require.NoError(t, err)
	dsn.User = url.UserPassword("limited_login", "test-password")
	pool, err := NewClientDatabasesPool(ctx, dsn.String())
	require.NoError(t, err)
	t.Cleanup(pool.Default(ctx).db.Close)

	database, err := pool.Default(ctx).GetDatabaseByName(ctx, "restricted_database")
	require.NoError(t, err)
	client, _, err := pool.Get(ctx, fmt.Sprint(database.ID))
	require.Nil(t, client)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "42501", pgErr.Code)
}

func TestClassifyConnectError(t *testing.T) {
	cases := []struct {
		name     string
		sqlstate string
		want     codes.Code
	}{
		{name: "invalid password", sqlstate: "28P01", want: codes.Unauthenticated},
		{name: "invalid authorization specification", sqlstate: "28000", want: codes.Unauthenticated},
		{name: "insufficient privilege", sqlstate: "42501", want: codes.PermissionDenied},
		{name: "unrelated server error", sqlstate: "3D000", want: codes.Unknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// pgx wraps the server error the same way ConnectConfig does.
			pgErr := &pgconn.PgError{Code: tc.sqlstate, Message: "server said no"}
			err := fmt.Errorf("failed to connect: %w", pgErr)

			got := classifyConnectError(err)

			require.Equal(t, tc.want, status.Code(got))
			var unwrapped *pgconn.PgError
			require.True(t, errors.As(got, &unwrapped), "original PgError must stay in the chain")
			require.Equal(t, tc.sqlstate, unwrapped.Code)
			require.Contains(t, got.Error(), "server said no")
		})
	}
}

func TestClassifyConnectErrorPassesThroughNonServerErrors(t *testing.T) {
	err := errors.New("dial tcp: connection refused")

	got := classifyConnectError(err)

	require.Same(t, err, got)
	require.Equal(t, codes.Unknown, status.Code(got))
}
