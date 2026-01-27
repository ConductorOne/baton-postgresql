package connector

import (
	"fmt"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"

	connectorv2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestGrantRevokeView(t *testing.T) {
	ctx, syncer, manager, client := newTestConnector(t)

	err := syncer.Sync(ctx)
	require.NoError(t, err)
	err = syncer.Close(ctx)
	require.NoError(t, err)

	c1z, err := manager.LoadC1Z(ctx)
	require.NoError(t, err)
	defer func(c1z *dotc1z.C1File) {
		err := c1z.Close(ctx)
		require.NoError(t, err)
	}(c1z)

	dbResource, err := getByDisplayName(ctx, c1z, databaseResourceType, "postgres")
	require.NoError(t, err)
	require.NotNil(t, dbResource)

	roleResource, err := getByDisplayName(ctx, c1z, roleResourceType, "test_role")
	require.NoError(t, err)
	require.NotNil(t, roleResource)

	viewResource, err := getByDisplayName(ctx, c1z, viewResourceType, "test_table_view")
	require.NoError(t, err)
	require.NotNil(t, viewResource)

	dbId, rId, err := parseWithDatabaseID(viewResource.Id.Resource)
	require.NoError(t, err)

	grantResponse, err := client.Grant(ctx, &connectorv2.GrantManagerServiceGrantRequest{
		Principal: &connectorv2.Resource{
			Id:          roleResource.Id,
			DisplayName: roleResource.DisplayName,
		},
		Entitlement: &connectorv2.Entitlement{
			Id: fmt.Sprintf("entitlement:view:db%s:%d:select:grant", dbId, rId),
			Resource: &connectorv2.Resource{
				Id: &connectorv2.ResourceId{
					ResourceType: viewResourceType.Id,
					Resource:     fmt.Sprintf("view:db%s:%d", dbId, rId),
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
