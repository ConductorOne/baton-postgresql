package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var databaseResourceType = &v2.ResourceType{
	Id:          "database",
	DisplayName: "Database",
	Traits:      nil,
	Annotations: nil,
}

type databaseSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *databaseSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return databaseResourceType
}

func (r *databaseSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID != nil {
		return nil, "", nil, fmt.Errorf("unexpected parent resource ID on database")
	}

	databases, nextPageToken, err := r.client.ListDatabases(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range databases {
		var annos annotations.Annotations

		annos.Append(&v2.ChildResourceType{ResourceTypeId: schemaResourceType.Id})

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

func (r *databaseSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(ctx, resource, postgres.Create|postgres.Temporary|postgres.Connect)
	if err != nil {
		return nil, "", nil, err
	}

	ens = append(ens, &v2.Entitlement{
		Resource:    resource,
		Id:          formatEntitlementID(resource, "superuser", false),
		DisplayName: "Superuser",
		Description: "Has Superuser access",
		GrantableTo: []*v2.ResourceType{roleResourceType},
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Slug:        "superuser",
	})

	ens = append(ens, &v2.Entitlement{
		Resource:    resource,
		Id:          formatEntitlementID(resource, "create-db", false),
		DisplayName: "Create Database",
		Description: "Can create new databases",
		GrantableTo: []*v2.ResourceType{roleResourceType},
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Slug:        "create db",
	})

	ens = append(ens, &v2.Entitlement{
		Resource:    resource,
		Id:          formatEntitlementID(resource, "create-role", false),
		DisplayName: "Create Role",
		Description: "Can create new roles",
		GrantableTo: []*v2.ResourceType{roleResourceType},
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Slug:        "create role",
	})

	ens = append(ens, &v2.Entitlement{
		Resource:    resource,
		Id:          formatEntitlementID(resource, "bypass-rls", false),
		DisplayName: "Bypass RLS",
		Description: "Can bypass row level security options",
		GrantableTo: []*v2.ResourceType{roleResourceType},
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Slug:        "bypass rls",
	})

	ens = append(ens, &v2.Entitlement{
		Resource:    resource,
		Id:          formatEntitlementID(resource, "replication", false),
		DisplayName: "Replication",
		Description: "Can initiate replication connections, and create and drop replication slots",
		GrantableTo: []*v2.ResourceType{roleResourceType},
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Slug:        "bypass rls",
	})

	return ens, "", nil, nil
}

func (r *databaseSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, err := parsePageToken(pToken.Token, resource.Id)
	if err != nil {
		return nil, "", nil, err
	}

	rID, err := parseObjectID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	switch bag.ResourceTypeID() {
	case databaseResourceType.Id:
		bag.Pop()

		db, err := r.client.GetDatabase(ctx, rID)
		if err != nil {
			return nil, "", nil, err
		}

		ret, err := grantsForPrivs(ctx, resource, r.client, db.ACLs, postgres.Create|postgres.Temporary|postgres.Connect)
		if err != nil {
			return nil, "", nil, err
		}

		bag.Push(pagination.PageState{
			ResourceTypeID: roleResourceType.Id,
		})

		nextPage, err := bag.Marshal()
		if err != nil {
			return nil, "", nil, err
		}

		return ret, nextPage, nil, nil

	case roleResourceType.Id:
		var ret []*v2.Grant
		roles, nextPage, err := r.client.ListRoles(ctx, &postgres.Pager{Token: bag.PageToken(), Size: pToken.Size})
		if err != nil {
			return nil, "", nil, err
		}

		nextPageToken, err := bag.NextToken(nextPage)
		if err != nil {
			return nil, "", nil, err
		}

		for _, r := range roles {
			principal := &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: roleResourceType.Id,
					Resource:     formatObjectID(roleResourceType.Id, r.ID),
				},
			}

			if r.Superuser {
				eID := formatEntitlementID(resource, "superuser", false)
				ret = append(ret, &v2.Grant{
					Entitlement: &v2.Entitlement{
						Id:       eID,
						Resource: resource,
					},
					Principal: principal,
					Id:        formatGrantID(eID, principal.Id),
				})
			}

			if r.CreateDb {
				eID := formatEntitlementID(resource, "create-db", false)
				ret = append(ret, &v2.Grant{
					Entitlement: &v2.Entitlement{
						Id:       eID,
						Resource: resource,
					},
					Principal: principal,
					Id:        formatGrantID(eID, principal.Id),
				})
			}

			if r.CreateRole {
				eID := formatEntitlementID(resource, "create-role", false)
				ret = append(ret, &v2.Grant{
					Entitlement: &v2.Entitlement{
						Id:       eID,
						Resource: resource,
					},
					Principal: principal,
					Id:        formatGrantID(eID, principal.Id),
				})
			}

			if r.BypassRowSecurity {
				eID := formatEntitlementID(resource, "bypass-rls", false)
				ret = append(ret, &v2.Grant{
					Entitlement: &v2.Entitlement{
						Id:       eID,
						Resource: resource,
					},
					Principal: principal,
					Id:        formatGrantID(eID, principal.Id),
				})
			}

			if r.Replication {
				eID := formatEntitlementID(resource, "replication", false)
				ret = append(ret, &v2.Grant{
					Entitlement: &v2.Entitlement{
						Id:       eID,
						Resource: resource,
					},
					Principal: principal,
					Id:        formatGrantID(eID, principal.Id),
				})
			}
		}

		return ret, nextPageToken, nil, nil

	default:
		return nil, "", nil, fmt.Errorf("unexpected pagination state for database grants")
	}
}

func newDatabaseSyncer(ctx context.Context, c *postgres.Client) *databaseSyncer {
	return &databaseSyncer{
		resourceType: databaseResourceType,
		client:       c,
	}
}
