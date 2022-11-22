package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var databaseResourceType = &v2.ResourceType{
	Id:          "database",
	DisplayName: "Database",
	Traits:      nil,
	Annotations: nil,
}

type databaseSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *databaseSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return databaseResourceType
}

func (r *databaseSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID != nil {
		return nil, "", nil, fmt.Errorf("unexpected parent resource ID on database")
	}

	databases, nextPageToken, err := r.client.ListDatabases(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range databases {
		var annos annotations.Annotations

		annos.Append(&v2.ChildResourceType{ResourceTypeId: schemaResourceType.Id})

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatObjectID(r.resourceType.Id, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *databaseSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (r *databaseSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newDatabaseSyncer(ctx context.Context, c *postgres.Client) *databaseSyncer {
	return &databaseSyncer{
		resourceType: databaseResourceType,
		client:       c,
	}
}
