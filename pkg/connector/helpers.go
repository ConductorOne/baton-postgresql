package connector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func formatWithDatabaseID(resourceTypeID string, dbId string, id int64) string {
	return fmt.Sprintf("%s:db%s:%d", resourceTypeID, dbId, id)
}

// parseWithDatabaseID return databaseId and schemaId.
func parseWithDatabaseID(id string) (string, int64, error) {
	parts := strings.SplitN(id, ":", 3)
	if len(parts) != 3 {
		return "", 0, fmt.Errorf("invalid object ID for database %s", id)
	}

	if len(parts[1]) < 2 {
		return "", 0, fmt.Errorf("invalid object ID for database %s expected prefix db", id)
	}

	schemaId, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", 0, errors.Join(err, fmt.Errorf("invalid object ID for database %s", id))
	}

	return parts[1][2:], schemaId, nil
}

func formatObjectID(resourceTypeID string, id int64) string {
	return fmt.Sprintf("%s:%d", resourceTypeID, id)
}

func formatColumnID(db string, tableID int64, columnID int64) string {
	return fmt.Sprintf("%s:%s:%d:%d", db, columnResourceType.Id, tableID, columnID)
}

func parseObjectID(id string) (int64, error) {
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid object ID %s", id)
	}

	return strconv.ParseInt(parts[1], 10, 64)
}

func parseColumnID(id string) (string, int64, int64, error) {
	parts := strings.SplitN(id, ":", 4)
	if len(parts) != 4 {
		return "", 0, 0, fmt.Errorf("invalid column ID %s", id)
	}

	db := parts[0]

	tID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", 0, 0, err
	}

	colID, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return "", 0, 0, err
	}

	return db, tID, colID, nil
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

func parseEntitlementID(id string) (string, string, string, bool, error) {
	parts := strings.SplitN(id, ":", 5)

	if len(parts) == 4 {
		return parts[1], parts[2], parts[3], false, nil
	}

	if len(parts) == 5 && parts[4] == "grant" {
		return parts[1], parts[2], parts[3], true, nil
	}

	return "", "", "", false, fmt.Errorf("invalid entilement ID %s %d", id, len(parts))
}

func grantsForPrivilegeSet(
	ctx context.Context,
	resource *v2.Resource,
	principal *v2.Resource,
	privs postgres.PrivilegeSet,
	grantPrivs postgres.PrivilegeSet,
) ([]*v2.Grant, error) {
	var ret []*v2.Grant

	err := postgres.EmptyPrivilegeSet.Range(func(privilege postgres.PrivilegeSet) (bool, error) {
		entitlements, err := entitlementsForPrivs(ctx, resource, privilege)
		if err != nil {
			return false, err
		}

		if privs.Has(privilege) {
			ret = append(ret, &v2.Grant{
				Entitlement: entitlements[0],
				Principal:   principal,
				Id:          formatGrantID(entitlements[0].Id, principal.Id),
			})
		}

		if grantPrivs.Has(privilege) {
			ret = append(ret, &v2.Grant{
				Entitlement: entitlements[1],
				Principal:   principal,
				Id:          formatGrantID(entitlements[1].Id, principal.Id),
			})
		}

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func getInheritedACLs(
	ctx context.Context,
	client *postgres.Client,
	role *postgres.RoleModel,
	aclsByRole map[string][]*postgres.ACL,
) ([]*postgres.ACL, error) {
	var ret []*postgres.ACL

	if !role.Inherit {
		return nil, nil
	}

	for _, pID := range role.MemberOf {
		pRole, err := client.GetRole(ctx, pID)
		if err != nil {
			return nil, err
		}
		ret = append(ret, aclsByRole[pRole.Name]...)

		parentACLs, err := getInheritedACLs(ctx, client, pRole, aclsByRole)
		if err != nil {
			return nil, err
		}

		ret = append(ret, parentACLs...)
	}

	return ret, nil
}

func roleGrantsForPrivileges(
	ctx context.Context,
	client *postgres.Client,
	resource *v2.Resource,
	roles []*postgres.RoleModel,
	aclObj postgres.ACLResource,
) ([]*v2.Grant, error) {
	var ret []*v2.Grant

	aclsByRole := make(map[string][]*postgres.ACL)

	var defaultACL *postgres.ACL
	for _, pgACL := range aclObj.GetACLs() {
		acl, err := postgres.NewACL(pgACL)
		if err != nil {
			return nil, err
		}

		grantee := acl.Grantee()
		if grantee == "" {
			defaultACL = acl
			continue
		}

		roleACLs, ok := aclsByRole[grantee]
		if ok {
			aclsByRole[grantee] = append(roleACLs, acl)
		} else {
			aclsByRole[grantee] = []*postgres.ACL{acl}
		}
	}

	if defaultACL == nil {
		defaultACL = postgres.NewACLFromPrivilegeSets(aclObj.DefaultPrivileges(), postgres.EmptyPrivilegeSet)
	}

	for _, r := range roles {
		privs := defaultACL.Privileges()
		grantPrivs := defaultACL.GrantPrivileges()

		roleACLs := aclsByRole[r.Name]

		inheritedACLs, err := getInheritedACLs(ctx, client, r, aclsByRole)
		if err != nil {
			return nil, err
		}

		roleACLs = append(roleACLs, inheritedACLs...)

		// If the role is a super user or the owner of the object, they get all privileges
		if r.Superuser || r.ID == aclObj.GetOwnerID() {
			privs = aclObj.AllPrivileges()
			grantPrivs = aclObj.AllPrivileges()
		}

		// Set the ACL privs appropriately
		for _, ra := range roleACLs {
			privs |= ra.Privileges()
			grantPrivs |= ra.GrantPrivileges()
		}

		principal := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: roleResourceType.Id,
				Resource:     formatObjectID(roleResourceType.Id, r.ID),
			},
		}

		grants, err := grantsForPrivilegeSet(ctx, resource, principal, privs, grantPrivs)
		if err != nil {
			return nil, err
		}

		ret = append(ret, grants...)
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
				Slug:        "grant " + slug,
			})
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return ret, nil
}
