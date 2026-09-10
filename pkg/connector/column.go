package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

var columnResourceType = &v2.ResourceType{
	Id:          "column",
	DisplayName: "Column",
	Traits:      nil,
	Annotations: nil,
}

type columnSyncer struct {
	resourceType *v2.ResourceType
	clientPool   *postgres.ClientDatabasesPool
}

func (r *columnSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return columnResourceType
}

func (r *columnSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	var err error
	pToken := &opts.PageToken

	if parentResourceID == nil {
		return nil, &resource.SyncOpResults{}, nil
	}

	if parentResourceID.ResourceType != tableResourceType.Id {
		return nil, nil, fmt.Errorf("invalid parent resource ID on column %s %s", parentResourceID.ResourceType, parentResourceID.Resource)
	}

	db, parentID, err := parseWithDatabaseID(parentResourceID.Resource)
	if err != nil {
		return nil, nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	columns, nextPageToken, err := client.ListColumns(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, nil, err
	}

	var ret []*v2.Resource
	for _, o := range columns {
		var annos annotations.Annotations

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatColumnID(db, parentID, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, &resource.SyncOpResults{NextPageToken: nextPageToken}, nil
}

func (r *columnSyncer) Entitlements(ctx context.Context, res *v2.Resource, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	col := &postgres.ColumnModel{}
	ens, err := entitlementsForPrivs(ctx, res, col.AllPrivileges())
	if err != nil {
		return nil, nil, err
	}

	return ens, &resource.SyncOpResults{}, nil
}

func (r *columnSyncer) Grants(ctx context.Context, res *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	pToken := &opts.PageToken
	db, tID, cID, err := parseColumnID(res.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	roles, nextPageToken, err := client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, nil, err
	}

	col, err := client.GetColumn(ctx, tID, cID)
	if err != nil {
		return nil, nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, client, res, roles, col)
	if err != nil {
		return nil, nil, err
	}

	return ret, &resource.SyncOpResults{NextPageToken: nextPageToken}, nil
}

func newColumnSyncer(ctx context.Context, c *postgres.ClientDatabasesPool) *columnSyncer {
	return &columnSyncer{
		resourceType: columnResourceType,
		clientPool:   c,
	}
}
