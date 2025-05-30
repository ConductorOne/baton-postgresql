package postgres

import (
	"context"
	"testing"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestProcedureGrantRevoke(t *testing.T) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	client, err := New(ctx, container.Dsn())
	assert.NoError(t, err)

	// Is grant true
	err = client.GrantProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), true)
	assert.NoError(t, err)

	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), true)
	assert.NoError(t, err)

	// is grant false
	err = client.GrantProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), false)
	assert.NoError(t, err)

	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), false)
	assert.NoError(t, err)

	// revoke without grant
	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), false)
	assert.NoError(t, err)

	err = client.RevokeProcedure(ctx, "public", "add_test_item", container.Role(), Execute.Name(), true)
	assert.NoError(t, err)
}
