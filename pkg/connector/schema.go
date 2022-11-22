package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var schemaResourceType = &v2.ResourceType{
	Id:          "schema",
	DisplayName: "Schema",
	Traits:      nil,
	Annotations: nil,
}

type schemaSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *schemaSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return schemaResourceType
}

func (r *schemaSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != databaseResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on schema")
	}

	schemas, nextPageToken, err := r.client.ListSchemas(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range schemas {
		var annos annotations.Annotations

		annos.Append(&v2.ChildResourceType{ResourceTypeId: tableResourceType.Id})
		annos.Append(&v2.ChildResourceType{ResourceTypeId: viewResourceType.Id})
		annos.Append(&v2.ChildResourceType{ResourceTypeId: functionResourceType.Id})
		annos.Append(&v2.ChildResourceType{ResourceTypeId: procedureResourceType.Id})

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

func (r *schemaSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(ctx, resource, postgres.Usage|postgres.Create)
	if err != nil {
		return nil, "", nil, err
	}

	return ens, "", nil, nil
}

func (r *schemaSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newSchemaSyncer(ctx context.Context, c *postgres.Client) *schemaSyncer {
	return &schemaSyncer{
		resourceType: schemaResourceType,
		client:       c,
	}
}
