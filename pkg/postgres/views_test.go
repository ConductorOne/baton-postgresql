package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
)

func TestViewGrantRevoke(t *testing.T) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	client, err := New(ctx, container.Dsn())
	require.NoError(t, err)

	// Is grant true
	err = client.GrantView(ctx, "public", "test_table_view", container.Role(), Select.Name(), true)
	require.NoError(t, err)

	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), true)
	require.NoError(t, err)

	// is grant false
	err = client.GrantView(ctx, "public", "test_table_view", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	// revoke without grant
	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), true)
	require.NoError(t, err)
}
