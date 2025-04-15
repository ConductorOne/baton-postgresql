package connector

import (
	"context"
	"fmt"
	"strconv"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var schemaResourceType = &v2.ResourceType{
	Id:          "schema",
	DisplayName: "Schema",
	Traits:      nil,
	Annotations: nil,
}

type schemaSyncer struct {
	resourceType *v2.ResourceType
	clientPool   *postgres.ClientDatabasesPool
}

func (r *schemaSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return schemaResourceType
}

func (r *schemaSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != databaseResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on schema")
	}

	dbId, err := parseObjectID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, dbName, err := r.clientPool.Get(ctx, strconv.Itoa(int(dbId)))
	if err != nil {
		return nil, "", nil, err
	}

	if dbName == "" {
		return nil, "", nil, fmt.Errorf("database name not found for ID %d", dbId)

	}

	schemas, nextPageToken, err := client.ListSchemas(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range schemas {
		var annos annotations.Annotations

		annos.Append(&v2.ChildResourceType{ResourceTypeId: tableResourceType.Id})
		annos.Append(&v2.ChildResourceType{ResourceTypeId: viewResourceType.Id})
		annos.Append(&v2.ChildResourceType{ResourceTypeId: functionResourceType.Id})
		annos.Append(&v2.ChildResourceType{ResourceTypeId: procedureResourceType.Id})
		annos.Append(&v2.ChildResourceType{ResourceTypeId: sequenceResourceType.Id})

		ret = append(ret, &v2.Resource{
			DisplayName: fmt.Sprintf("%s - %s", dbName, o.Name),
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatWithDatabaseID(r.resourceType.Id, strconv.FormatInt(dbId, 10), o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *schemaSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(ctx, resource, postgres.Usage|postgres.Create)
	if err != nil {
		return nil, "", nil, err
	}

	return ens, "", nil, nil
}

func (r *schemaSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	db, rID, err := parseWithDatabaseID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, "", nil, err
	}

	schema, err := client.GetSchema(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, client, resource, roles, schema)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, nextPageToken, nil, nil
}

func newSchemaSyncer(ctx context.Context, c *postgres.ClientDatabasesPool) *schemaSyncer {
	return &schemaSyncer{
		resourceType: schemaResourceType,
		clientPool:   c,
	}
}
