package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/sdk"
)

var roleResourceType = &v2.ResourceType{
	Id:          "role",
	DisplayName: "Role",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	Annotations: nil,
}

type roleSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *roleSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return roleResourceType
}

func (r *roleSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID != nil {
		return nil, "", nil, fmt.Errorf("unexpected parent resource ID on role: %s", parentResourceID)
	}

	roles, nextPageToken, err := r.client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range roles {
		var annos annotations.Annotations

		p := make(map[string]interface{})

		gt, err := sdk.NewGroupTrait(nil, p)
		if err != nil {
			return nil, "", nil, err
		}
		annos.Append(gt)

		ut, err := sdk.NewUserTrait("", v2.UserTrait_Status_STATUS_ENABLED, nil, nil)
		if err != nil {
			return nil, "", nil, err
		}
		annos.Append(ut)

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

func (r *roleSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (r *roleSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newRoleSyncer(ctx context.Context, c *postgres.Client) *roleSyncer {
	return &roleSyncer{
		resourceType: roleResourceType,
		client:       c,
	}
}
