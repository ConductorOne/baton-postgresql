package connector

import (
	"fmt"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"

	connectorv2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestGrantRevokeProcedure(t *testing.T) {
	ctx, syncer, manager, client := newTestConnector(t)

	err := syncer.Sync(ctx)
	require.NoError(t, err)
	err = syncer.Close(ctx)
	require.NoError(t, err)

	c1z, err := manager.LoadC1Z(ctx)
	require.NoError(t, err)
	require.NoError(t, err)
	defer func(c1z *dotc1z.C1File) {
		err := c1z.Close()
		require.NoError(t, err)
	}(c1z)

	dbResource, err := getByDisplayName(ctx, c1z, databaseResourceType, "postgres")
	require.NoError(t, err)
	require.NotNil(t, dbResource)

	roleResource, err := getByDisplayName(ctx, c1z, roleResourceType, "test_role")
	require.NoError(t, err)
	require.NotNil(t, roleResource)

	procedureResource, err := getByDisplayName(ctx, c1z, procedureResourceType, "add_test_item(IN item_name character varying)")
	require.NoError(t, err)
	require.NotNil(t, procedureResource)

	dbId, rId, err := parseWithDatabaseID(procedureResource.Id.Resource)
	require.NoError(t, err)

	grantResponse, err := client.Grant(ctx, &connectorv2.GrantManagerServiceGrantRequest{
		Principal: &connectorv2.Resource{
			Id:          roleResource.Id,
			DisplayName: roleResource.DisplayName,
		},
		Entitlement: &connectorv2.Entitlement{
			Id: fmt.Sprintf("entitlement:procedure:db%s:%d:execute", dbId, rId),
			Resource: &connectorv2.Resource{
				Id: &connectorv2.ResourceId{
					ResourceType: procedureResourceType.Id,
					Resource:     fmt.Sprintf("procedure:db%s:%d", dbId, rId),
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
