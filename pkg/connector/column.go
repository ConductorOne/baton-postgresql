package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var columnResourceType = &v2.ResourceType{
	Id:          "column",
	DisplayName: "Column",
	Traits:      nil,
	Annotations: nil,
}

type columnSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *columnSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return columnResourceType
}

func (r *columnSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != tableResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on column %s %s", parentResourceID.ResourceType, parentResourceID.Resource)
	}

	parentID, err := parseObjectID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	columns, nextPageToken, err := r.client.ListColumns(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range columns {
		var annos annotations.Annotations

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatColumnID(parentID, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *columnSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(ctx, resource, postgres.Insert|postgres.Select|postgres.Update|postgres.References)
	if err != nil {
		return nil, "", nil, err
	}

	return ens, "", nil, nil
}

func (r *columnSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	tID, cID, err := parseColumnID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	col, err := r.client.GetColumn(ctx, tID, cID)
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := grantsForPrivs(ctx, resource, r.client, col.ACLs, postgres.Insert|postgres.Select|postgres.Update|postgres.References)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, "", nil, nil
}

func newColumnSyncer(ctx context.Context, c *postgres.Client) *columnSyncer {
	return &columnSyncer{
		resourceType: columnResourceType,
		client:       c,
	}
}
