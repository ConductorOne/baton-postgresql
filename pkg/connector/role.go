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

		hasMembers, err := r.client.RoleHasMembers(ctx, o.ID)
		if err != nil {
			return nil, "", nil, err
		}

		if hasMembers {
			gt, err := sdk.NewGroupTrait(nil, p)
			if err != nil {
				return nil, "", nil, err
			}
			annos.Append(gt)
		}

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
	var ret []*v2.Entitlement

	annos := annotations.Annotations(resource.Annotations)

	gt := &v2.GroupTrait{}
	ok, err := annos.Pick(gt)
	if err != nil {
		return nil, "", nil, err
	}

	if ok {
		ret = append(ret, &v2.Entitlement{
			Resource:    resource,
			Id:          formatEntitlementID(resource, "member", false),
			DisplayName: "Member",
			Description: fmt.Sprintf("Is assigned the %s role", resource.DisplayName),
			GrantableTo: []*v2.ResourceType{roleResourceType},
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
			Slug:        "member",
		})
		ret = append(ret, &v2.Entitlement{
			Resource:    resource,
			Id:          formatEntitlementID(resource, "admin", false),
			DisplayName: "Admin",
			Description: fmt.Sprintf("Can grant the %s role to other roles", resource.DisplayName),
			GrantableTo: []*v2.ResourceType{roleResourceType},
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
			Slug:        "admin",
		})
	}

	return ret, "", nil, nil
}

func (r *roleSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var ret []*v2.Grant

	annos := annotations.Annotations(resource.Annotations)
	gt := &v2.GroupTrait{}
	ok, err := annos.Pick(gt)
	if err != nil {
		return nil, "", nil, err
	}

	// Roles only have entitlements if they are a group
	if !ok {
		return nil, "", nil, nil
	}

	roleID, err := parseObjectID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	roleMembers, nextPageToken, err := r.client.ListRoleMembers(ctx, roleID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var eID string
	for _, m := range roleMembers {
		if m.IsRoleAdmin() {
			eID = formatEntitlementID(resource, "admin", false)
		} else {
			eID = formatEntitlementID(resource, "member", false)
		}

		principal := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: roleResourceType.Id,
				Resource:     formatObjectID(roleResourceType.Id, m.ID),
			},
		}
		ret = append(ret, &v2.Grant{
			Id: formatGrantID(eID, principal.Id),
			Entitlement: &v2.Entitlement{
				Id:       eID,
				Resource: resource,
			},
			Principal: principal,
		})
	}

	return ret, nextPageToken, nil, nil
}

func newRoleSyncer(ctx context.Context, c *postgres.Client) *roleSyncer {
	return &roleSyncer{
		resourceType: roleResourceType,
		client:       c,
	}
}
