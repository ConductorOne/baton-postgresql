package postgres

import (
	"github.com/conductorone/baton-postgresql/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTableGrantRevoke(t *testing.T) {
	ctx := t.Context()

	container := testutil.SetupPostgresContainer(t)

	client, err := New(ctx, container.Dsn())
	assert.NoError(t, err)

	// Is grant true
	err = client.GrantTable(ctx, "public", "test_table", container.Role(), Select.Name(), true)
	assert.NoError(t, err)

	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), true)
	assert.NoError(t, err)

	// is grant false
	err = client.GrantTable(ctx, "public", "test_table", container.Role(), Select.Name(), false)
	assert.NoError(t, err)

	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), false)
	assert.NoError(t, err)

	// revoke without grant
	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), false)
	assert.NoError(t, err)

	err = client.RevokeTable(ctx, "public", "test_table", container.Role(), Select.Name(), true)
	assert.NoError(t, err)
}
