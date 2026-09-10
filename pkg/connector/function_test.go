package connector

import (
	"fmt"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"

	connectorv2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestGrantRevokeFunction(t *testing.T) {
	ctx, syncer, c1zPath, client := newTestConnector(t)

	err := syncer.Sync(ctx)
	require.NoError(t, err)
	err = syncer.Close(ctx)
	require.NoError(t, err)

	c1z, err := dotc1z.NewStore(ctx, c1zPath)
	require.NoError(t, err)
	require.NoError(t, err)
	defer func(c1z c1zstore.Store) {
		err := c1z.Close(ctx)
		require.NoError(t, err)
	}(c1z)

	dbResource, err := getByDisplayName(ctx, c1z, databaseResourceType, "postgres")
	require.NoError(t, err)
	require.NotNil(t, dbResource)

	roleResource, err := getByDisplayName(ctx, c1z, roleResourceType, "test_role")
	require.NoError(t, err)
	require.NotNil(t, roleResource)

	functionResource, err := getByDisplayName(ctx, c1z, functionResourceType, "get_test_item_count()")
	require.NoError(t, err)
	require.NotNil(t, functionResource)

	dbId, rId, err := parseWithDatabaseID(functionResource.Id.Resource)
	require.NoError(t, err)

	grantResponse, err := client.Grant(ctx, &connectorv2.GrantManagerServiceGrantRequest{
		Principal: &connectorv2.Resource{
			Id:          roleResource.Id,
			DisplayName: roleResource.DisplayName,
		},
		Entitlement: &connectorv2.Entitlement{
			Id: fmt.Sprintf("entitlement:function:db%s:%d:execute", dbId, rId),
			Resource: &connectorv2.Resource{
				Id: &connectorv2.ResourceId{
					ResourceType: functionResourceType.Id,
					Resource:     fmt.Sprintf("function:db%s:%d", dbId, rId),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, grantResponse)
	require.Len(t, grantResponse.Grants, 1)

	grant := grantResponse.Grants[0]

	revokeResponse, err := client.Revoke(ctx, &connectorv2.GrantManagerServiceRevokeRequest{
		Grant: grant,
	})
	require.NoError(t, err)
	require.NotNil(t, revokeResponse)
}
