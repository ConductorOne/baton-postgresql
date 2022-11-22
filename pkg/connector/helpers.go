package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

func formatObjectID(resourceTypeID string, id int64) string {
	return fmt.Sprintf("%s:%d", resourceTypeID, id)
}

func formatColumnID(tableID int64, columnID int64) string {
	return fmt.Sprintf("%s:%d:%d", columnResourceType.Id, tableID, columnID)
}

func parseObjectID(id string) (int64, error) {
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid object ID %s", id)
	}

	return strconv.ParseInt(parts[1], 10, 64)
}

func parsePageToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	return b, nil
}

func formatGrantID(entitlementID string, principalId *v2.ResourceId) string {
	return fmt.Sprintf(
		"grant:%s:%s",
		entitlementID,
		principalId.Resource,
	)
}

func formatEntitlementID(resource *v2.Resource, privName string, grant bool) string {
	if grant {
		return fmt.Sprintf("entitlement:%s:%s:grant", resource.Id.Resource, privName)
	} else {
		return fmt.Sprintf("entitlement:%s:%s", resource.Id.Resource, privName)
	}
}

func grantsForPrivs(
	ctx context.Context,
	resource *v2.Resource,
	client *postgres.Client,
	acls []string,
	set postgres.PrivilegeSet,
) ([]*v2.Grant, error) {
	var ret []*v2.Grant

	for _, pgAcl := range acls {
		acl, err := postgres.NewAcl(pgAcl)
		if err != nil {
			return nil, err
		}

		if acl.Grantee() == "" {
			fmt.Println("FIXME: PUBLIC GRANTS SKIPPING", pgAcl)
			continue
		}

		grantee, err := client.GetRoleByName(ctx, acl.Grantee())
		if err != nil {
			return nil, err
		}

		granteeResource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: roleResourceType.Id,
				Resource:     formatObjectID(roleResourceType.Id, grantee.ID),
			},
		}

		// FIXME: better range helper
		err = postgres.PrivilegeSet(0).Range(func(privilege postgres.PrivilegeSet) (bool, error) {
			if set.Has(privilege) {
				hasPriv, hasPrivGrant := acl.Check(privilege)
				entitlements, err := entitlementsForPrivs(ctx, resource, privilege)
				if err != nil {
					return false, err
				}
				if hasPriv || grantee.Superuser {
					ret = append(ret, &v2.Grant{
						Entitlement: entitlements[0],
						Principal:   granteeResource,
						Id:          formatGrantID(entitlements[0].Id, granteeResource.Id),
					})
				}

				if hasPrivGrant || grantee.Superuser {
					ret = append(ret, &v2.Grant{
						Entitlement: entitlements[1],
						Principal:   granteeResource,
						Id:          formatGrantID(entitlements[1].Id, granteeResource.Id),
					})
				}
			}
			return true, nil
		})
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func entitlementsForPrivs(ctx context.Context, resource *v2.Resource, privs postgres.PrivilegeSet) ([]*v2.Entitlement, error) {
	var ret []*v2.Entitlement
	err := privs.Range(func(p postgres.PrivilegeSet) (bool, error) {
		if privs.Has(p) {
			slug := strings.ToLower(p.Name())
			ret = append(ret, &v2.Entitlement{
				Resource:    resource,
				Id:          formatEntitlementID(resource, slug, false),
				DisplayName: p.Name(),
				Description: fmt.Sprintf("Has %s privileges on %s", p.Name(), resource.DisplayName),
				GrantableTo: []*v2.ResourceType{roleResourceType},
				Annotations: nil,
				Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
				Slug:        slug,
			})
			ret = append(ret, &v2.Entitlement{
				Resource:    resource,
				Id:          formatEntitlementID(resource, slug, true),
				DisplayName: fmt.Sprintf("Can grant %s", p.Name()),
				Description: fmt.Sprintf("Can grant %s privileges on %s", p.Name(), resource.DisplayName),
				GrantableTo: []*v2.ResourceType{roleResourceType},
				Annotations: nil,
				Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
				Slug:        "grant: " + slug,
			})
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return ret, nil
}
