package connector

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/jackc/pgconn"
	"go.uber.org/zap"

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
	clientPool   *postgres.ClientDatabasesPool
	client       *postgres.Client
}

func (r *databaseSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return databaseResourceType
}

func (r *databaseSyncer) makeResource(ctx context.Context, dbModel *postgres.DatabaseModel) *v2.Resource {
	var annos annotations.Annotations

	annos.Append(&v2.ChildResourceType{ResourceTypeId: schemaResourceType.Id})

	return &v2.Resource{
		DisplayName: dbModel.Name,
		Id: &v2.ResourceId{
			ResourceType: r.resourceType.Id,
			Resource:     formatObjectID(r.resourceType.Id, dbModel.ID),
		},
		Annotations: annos,
	}
}

func (r *databaseSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	var err error

	if parentResourceID != nil {
		return nil, "", nil, fmt.Errorf("unexpected parent resource ID on database")
	}

	databases, nextPageToken, err := r.clientPool.
		Default(ctx).
		ListDatabases(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})

	if err != nil {
		return nil, "", nil, err
	}

	defaultDatabase := r.clientPool.DefaultDatabase(ctx)

	var ret []*v2.Resource
	for _, o := range databases {
		if defaultDatabase != "" && o.Name != defaultDatabase {
			continue
		}

		_, _, err := r.clientPool.Get(ctx, fmt.Sprintf("%d", o.ID))
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				// Database does not accept connections
				if pgErr.Code == "55000" {
					l.Info("skipping database that does not accept connections", zap.String("database", o.Name), zap.Error(err))
					continue
				}
				l.Warn("skipping database with error", zap.String("database", o.Name), zap.Error(err))
				continue
			}
			return nil, "", nil, err
		}

		ret = append(ret, r.makeResource(ctx, o))
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
	rID, err := parseObjectID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	db, err := r.client.GetDatabase(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := r.client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, r.client, resource, roles, db)
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
}

func (r *databaseSyncer) Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	if resource.Id.ResourceType != databaseResourceType.Id {
		return nil, nil, fmt.Errorf("baton-postgres: non-database resource passed to database create")
	}

	dbName := resource.GetDisplayName()
	dbModel, err := r.client.CreateDatabase(ctx, dbName)
	if err != nil {
		return nil, nil, err
	}
	dbResource := r.makeResource(ctx, dbModel)
	return dbResource, nil, nil
}

func (r *databaseSyncer) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	if resourceId.ResourceType != databaseResourceType.Id {
		return nil, fmt.Errorf("baton-postgres: non-database resource passed to database delete")
	}

	dbId, err := parseObjectID(resourceId.Resource)
	if err != nil {
		return nil, err
	}

	pgDb, err := r.client.GetDatabase(ctx, dbId)
	if err != nil {
		return nil, err
	}

	err = r.client.DeleteDatabase(ctx, pgDb.Name)
	return nil, err
}

func (r *databaseSyncer) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, nil, fmt.Errorf("baton-postgres: only users and roles can have roles granted")
	}

	// Parse the Entitlement ID to get the database ID and privilege name
	_, dbIdStr, privilegeName, isGrant, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, nil, err
	}

	dbID, err := strconv.ParseInt(dbIdStr, 10, 64)
	if err != nil {
		return nil, nil, err
	}

	// Look up the database by ID
	pgDb, err := r.client.GetDatabase(ctx, dbID)
	if err != nil {
		return nil, nil, err
	}

	principalName := principal.DisplayName
	err = r.client.GrantDatabase(ctx, pgDb.Name, principalName, privilegeName, isGrant)
	return nil, nil, err
}

func (r *databaseSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	entitlement := grant.Entitlement
	principal := grant.Principal

	_, dbIdStr, privilegeName, isGrant, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, err
	}

	dbID, err := strconv.ParseInt(dbIdStr, 10, 64)
	if err != nil {
		return nil, err
	}

	pgDb, err := r.client.GetDatabase(ctx, dbID)
	if err != nil {
		return nil, err
	}

	principalName := principal.DisplayName
	err = r.client.RevokeDatabase(ctx, pgDb.Name, principalName, privilegeName, isGrant)
	return nil, err
}

func newDatabaseSyncer(ctx context.Context, c *postgres.ClientDatabasesPool) *databaseSyncer {
	return &databaseSyncer{
		resourceType: databaseResourceType,
		clientPool:   c,
		client:       c.Default(ctx),
	}
}
