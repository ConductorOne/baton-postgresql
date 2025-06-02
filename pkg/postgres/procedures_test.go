package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
)

func TestProcedureGrantRevoke(t *testing.T) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	client, err := New(ctx, container.Dsn())
	require.NoError(t, err)

	// Is grant true
	err = client.GrantProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), true)
	require.NoError(t, err)

	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), true)
	require.NoError(t, err)

	// is grant false
	err = client.GrantProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), false)
	require.NoError(t, err)

	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), false)
	require.NoError(t, err)

	// revoke without grant
	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), false)
	require.NoError(t, err)

	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), true)
	require.NoError(t, err)
}
