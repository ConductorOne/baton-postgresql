package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
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

func (r *viewSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on view")
	}

	db, parentID, err := parseWithDatabaseID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, "", nil, err
	}

	views, nextPageToken, err := client.ListViews(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
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

	return ret, nextPageToken, nil, nil
}

func (r *viewSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
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
		annos := annotations.Annotations(en.Annotations)
		annos.Update(&v2.EntitlementImmutable{})
		en.DisplayName = fmt.Sprintf("%s on %s", dbModel.Name, en.DisplayName)
	}

	return ens, "", nil, nil
}

func (r *viewSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	db, rID, err := parseWithDatabaseID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, "", nil, err
	}

	view, err := client.GetView(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, client, resource, roles, view)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, nextPageToken, nil, nil
}

func newViewSyncer(ctx context.Context, c *postgres.ClientDatabasesPool) *viewSyncer {
	return &viewSyncer{
		resourceType: viewResourceType,
		clientPool:   c,
	}
}
