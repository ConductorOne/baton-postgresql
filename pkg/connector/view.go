package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

var viewResourceType = &v2.ResourceType{
	Id:          "view",
	DisplayName: "View",
	Traits:      nil,
	Annotations: nil,
}

type viewSyncer struct {
	resourceType *v2.ResourceType
	clientPool   *postgres.ClientDatabasesPool
}

func (r *viewSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return viewResourceType
}

func (r *viewSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	var err error
	pToken := &opts.PageToken

	if parentResourceID == nil {
		return nil, &resource.SyncOpResults{}, nil
	}

	if parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, nil, fmt.Errorf("invalid parent resource ID on view")
	}

	db, parentID, err := parseWithDatabaseID(parentResourceID.Resource)
	if err != nil {
		return nil, nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	views, nextPageToken, err := client.ListViews(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, nil, err
	}

	var ret []*v2.Resource
	for _, o := range views {
		var annos annotations.Annotations

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatWithDatabaseID(viewResourceType.Id, db, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, &resource.SyncOpResults{NextPageToken: nextPageToken}, nil
}

func (r *viewSyncer) Entitlements(ctx context.Context, res *v2.Resource, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	dbId, _, err := parseWithDatabaseID(res.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	dbModel, err := r.clientPool.
		Default(ctx).
		GetDatabaseById(ctx, dbId)

	if err != nil {
		return nil, nil, err
	}

	ens, err := entitlementsForPrivs(
		ctx,
		res,
		postgres.Select|postgres.Insert|postgres.Update|postgres.Delete|postgres.Truncate|postgres.Trigger|postgres.References,
	)
	if err != nil {
		return nil, nil, err
	}

	for _, en := range ens {
		en.DisplayName = fmt.Sprintf("%s on %s", dbModel.Name, en.DisplayName)
	}

	return ens, &resource.SyncOpResults{}, nil
}

func (r *viewSyncer) Grants(ctx context.Context, res *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	pToken := &opts.PageToken
	db, rID, err := parseWithDatabaseID(res.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	view, err := client.GetView(ctx, rID)
	if err != nil {
		return nil, nil, err
	}

	roles, nextPageToken, err := client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, client, res, roles, view)
	if err != nil {
		return nil, nil, err
	}

	return ret, &resource.SyncOpResults{NextPageToken: nextPageToken}, nil
}

func (r *viewSyncer) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, nil, fmt.Errorf("baton-postgres: only users and roles can have view granted")
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

	view, err := dbClient.GetView(ctx, rID)
	if err != nil {
		return nil, nil, err
	}

	err = dbClient.GrantView(ctx, view.Schema, view.Name, principal.DisplayName, privilegeName, isGrant)
	if err != nil {
		return nil, nil, err
	}

	return []*v2.Grant{
		{
			Id:          fmt.Sprintf("%s:%s:%s", entitlement.Id, principal.Id.ResourceType, principal.Id.Resource),
			Entitlement: entitlement,
			Principal:   principal,
		},
	}, nil, nil
}

func (r *viewSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	entitlement := grant.Entitlement
	principal := grant.Principal

	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, fmt.Errorf("baton-postgres: only users and roles can have view revoked")
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

	view, err := dbClient.GetView(ctx, rID)
	if err != nil {
		return nil, err
	}

	err = dbClient.RevokeView(ctx, view.Schema, view.Name, principal.DisplayName, privilegeName, isGrant)
	return nil, err
}

func newViewSyncer(ctx context.Context, c *postgres.ClientDatabasesPool) *viewSyncer {
	return &viewSyncer{
		resourceType: viewResourceType,
		clientPool:   c,
	}
}
