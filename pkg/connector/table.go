package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var tableResourceType = &v2.ResourceType{
	Id:          "table",
	DisplayName: "Table",
	Traits:      nil,
	Annotations: nil,
}

type tableSyncer struct {
	resourceType   *v2.ResourceType
	clientPool     *postgres.ClientDatabasesPool
	includeColumns bool
}

func (r *tableSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return tableResourceType
}

func (r *tableSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil || pToken == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on table")
	}

	database, parentID, err := parseWithDatabaseID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, _, err := r.clientPool.Get(ctx, database)
	if err != nil {
		return nil, "", nil, err
	}

	schema, err := client.GetSchema(ctx, parentID)
	if err != nil {
		return nil, "", nil, err
	}

	tables, nextPageToken, err := client.ListTables(ctx, schema.Name, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range tables {
		var annos annotations.Annotations

		if r.includeColumns {
			annos.Append(&v2.ChildResourceType{ResourceTypeId: columnResourceType.Id})
		}

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatWithDatabaseID(tableResourceType.Id, database, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *tableSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	dbId, _, err := parseWithDatabaseID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	dbModel, err := r.clientPool.
		Default(ctx).
		GetDatabaseById(ctx, dbId)

	if err != nil {
		return nil, "", nil, err
	}

	ens, err := entitlementsForPrivs(
		ctx,
		resource,
		postgres.Select|postgres.Insert|postgres.Update|postgres.Delete|postgres.Truncate|postgres.Trigger|postgres.References,
	)
	if err != nil {
		return nil, "", nil, err
	}

	for _, en := range ens {
		en.DisplayName = fmt.Sprintf("%s - %s", dbModel.Name, resource.DisplayName)
	}

	return ens, "", nil, nil
}

func (r *tableSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	db, rID, err := parseWithDatabaseID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, "", nil, err
	}

	table, err := client.GetTable(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, client, resource, roles, table)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, nextPageToken, nil, nil
}

func (r *tableSyncer) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, nil, fmt.Errorf("baton-postgres: only users and roles can have roles granted")
	}

	_, _, privilegeName, isGrant, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, nil, err
	}

	dbId, rID, err := parseWithDatabaseID(entitlement.Resource.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	dbClient, _, err := r.clientPool.Get(ctx, dbId)
	if err != nil {
		return nil, nil, err
	}

	table, err := dbClient.GetTable(ctx, rID)
	if err != nil {
		return nil, nil, err
	}

	err = dbClient.GrantTable(ctx, table.Schema, table.Name, principal.DisplayName, privilegeName, isGrant)
	return nil, nil, err
}

func (r *tableSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	entitlement := grant.Entitlement
	principal := grant.Principal

	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, fmt.Errorf("baton-postgres: only users and roles can have roles granted")
	}

	_, _, privilegeName, isGrant, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, err
	}

	dbId, rID, err := parseWithDatabaseID(entitlement.Resource.Id.Resource)
	if err != nil {
		return nil, err
	}

	dbClient, _, err := r.clientPool.Get(ctx, dbId)
	if err != nil {
		return nil, err
	}

	table, err := dbClient.GetTable(ctx, rID)
	if err != nil {
		return nil, err
	}

	err = dbClient.RevokeTable(ctx, table.Schema, table.Name, principal.DisplayName, privilegeName, isGrant)
	return nil, err
}

func newTableSyncer(ctx context.Context, c *postgres.ClientDatabasesPool, includeColumns bool) *tableSyncer {
	return &tableSyncer{
		resourceType:   tableResourceType,
		clientPool:     c,
		includeColumns: includeColumns,
	}
}
