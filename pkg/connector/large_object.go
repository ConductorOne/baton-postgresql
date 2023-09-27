package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var largeObjectResourceType = &v2.ResourceType{
	Id:          "large-object",
	DisplayName: "Large Object",
	Traits:      nil,
	Annotations: nil,
}

type largeObjectSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
	enabled      bool
}

func (r *largeObjectSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return largeObjectResourceType
}

func (r *largeObjectSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID != nil {
		return nil, "", nil, fmt.Errorf("unexpected parent resource ID on large object")
	}

	if !r.enabled {
		return nil, "", nil, nil
	}

	largeObjects, nextPageToken, err := r.client.ListLargeObjects(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range largeObjects {
		var annos annotations.Annotations

		ret = append(ret, &v2.Resource{
			DisplayName: fmt.Sprintf("%d", o.ID),
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

func (r *largeObjectSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(ctx, resource, postgres.Select|postgres.Update)
	if err != nil {
		return nil, "", nil, err
	}

	return ens, "", nil, nil
}

func (r *largeObjectSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	rID, err := parseObjectID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	largeObject, err := r.client.GetLargeObject(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := r.client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, r.client, resource, roles, largeObject)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, nextPageToken, nil, nil
}

func newLargeObjectSyncer(ctx context.Context, c *postgres.Client, enabled bool) *largeObjectSyncer {
	return &largeObjectSyncer{
		resourceType: largeObjectResourceType,
		client:       c,
		enabled:      enabled,
	}
}
