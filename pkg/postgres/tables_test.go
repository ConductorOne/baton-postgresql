package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
)

func TestTableGrantRevoke(t *testing.T) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	client, err := New(ctx, container.Dsn())
	require.NoError(t, err)

	// Is grant true
	err = client.GrantTable(ctx, "public", "test_table", container.Role(), Select.Name(), true)
	require.NoError(t, err)

	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), true)
	require.NoError(t, err)

	// is grant false
	err = client.GrantTable(ctx, "public", "test_table", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	// revoke without grant
	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), true)
	require.NoError(t, err)
}
