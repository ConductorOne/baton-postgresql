package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
)

func TestFunctionGrantRevoke(t *testing.T) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	client, err := New(ctx, container.Dsn())
	require.NoError(t, err)

	functionModel := &FunctionModel{Name: "get_test_item_count", Arguments: ""}

	// Is grant true
	err = client.GrantFunction(ctx, "public", functionModel, container.Role(), Execute.Name(), true)
	require.NoError(t, err)

	err = client.RevokeFunction(ctx, "public", functionModel, container.Role(), Execute.Name(), true)
	require.NoError(t, err)

	// is grant false
	err = client.GrantFunction(ctx, "public", functionModel, container.Role(), Execute.Name(), false)
	require.NoError(t, err)

	err = client.RevokeFunction(ctx, "public", functionModel, container.Role(), Execute.Name(), false)
	require.NoError(t, err)

	// revoke without grant
	err = client.RevokeFunction(ctx, "public", functionModel, container.Role(), Execute.Name(), false)
	require.NoError(t, err)

	err = client.RevokeFunction(ctx, "public", functionModel, container.Role(), Execute.Name(), true)
	require.NoError(t, err)
}
