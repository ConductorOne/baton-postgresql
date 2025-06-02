package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
)

func TestSequencesGrantRevoke(t *testing.T) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	client, err := New(ctx, container.Dsn())
	require.NoError(t, err)

	// Is grant true
	err = client.GrantSequence(ctx, "public", "test_table_seq", container.Role(), Select.Name(), true)
	require.NoError(t, err)

	err = client.RevokeSequence(ctx, "public", "test_table_seq", container.Role(), Select.Name(), true)
	require.NoError(t, err)

	// is grant false
	err = client.GrantSequence(ctx, "public", "test_table_seq", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	err = client.RevokeSequence(ctx, "public", "test_table_seq", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	// revoke without grant
	err = client.RevokeSequence(ctx, "public", "test_table_seq", container.Role(), Select.Name(), false)
	require.NoError(t, err)

	err = client.RevokeSequence(ctx, "public", "test_table_seq", container.Role(), Select.Name(), true)
	require.NoError(t, err)
}
