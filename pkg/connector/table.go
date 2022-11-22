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
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *tableSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return tableResourceType
}

func (r *tableSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on table")
	}

	parentID, err := parseObjectID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	tables, nextPageToken, err := r.client.ListTables(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range tables {
		var annos annotations.Annotations

		annos.Append(&v2.ChildResourceType{ResourceTypeId: columnResourceType.Id})

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatObjectID(tableResourceType.Id, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *tableSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(
		ctx,
		resource,
		postgres.Select|postgres.Insert|postgres.Update|postgres.Delete|postgres.Truncate|postgres.Trigger|postgres.References,
	)
	if err != nil {
		return nil, "", nil, err
	}

	return ens, "", nil, nil
}

func (r *tableSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	rID, err := parseObjectID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	table, err := r.client.GetTable(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := r.client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, resource, roles, table)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, nextPageToken, nil, nil
}

func newTableSyncer(ctx context.Context, c *postgres.Client) *tableSyncer {
	return &tableSyncer{
		resourceType: tableResourceType,
		client:       c,
	}
}
