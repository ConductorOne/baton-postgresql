package postgres

import (
	"context"
	"testing"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestViewGrantRevoke(t *testing.T) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	client, err := New(ctx, container.Dsn())
	assert.NoError(t, err)

	// Is grant true
	err = client.GrantView(ctx, "public", "test_table_view", container.Role(), Select.Name(), true)
	assert.NoError(t, err)

	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), true)
	assert.NoError(t, err)

	// is grant false
	err = client.GrantView(ctx, "public", "test_table_view", container.Role(), Select.Name(), false)
	assert.NoError(t, err)

	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), false)
	assert.NoError(t, err)

	// revoke without grant
	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), false)
	assert.NoError(t, err)

	err = client.RevokeView(ctx, "public", "test_table_view", container.Role(), Select.Name(), true)
	assert.NoError(t, err)
}
