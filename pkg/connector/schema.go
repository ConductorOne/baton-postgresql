package connector

import (
	"context"
	"fmt"
	"strconv"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
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

func (r *schemaSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	var err error
	pToken := &opts.PageToken

	if parentResourceID == nil {
		return nil, &resource.SyncOpResults{}, nil
	}

	if parentResourceID.ResourceType != databaseResourceType.Id {
		return nil, nil, fmt.Errorf("invalid parent resource ID on schema")
	}

	dbId, err := parseObjectID(parentResourceID.Resource)
	if err != nil {
		return nil, nil, err
	}

	client, dbName, err := r.clientPool.Get(ctx, strconv.Itoa(int(dbId)))
	if err != nil {
		return nil, nil, err
	}

	if dbName == "" {
		return nil, nil, fmt.Errorf("database name not found for ID %d", dbId)
	}

	schemas, nextPageToken, err := client.ListSchemas(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, nil, err
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

	return ret, &resource.SyncOpResults{NextPageToken: nextPageToken}, nil
}

func (r *schemaSyncer) Entitlements(ctx context.Context, res *v2.Resource, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	ens, err := entitlementsForPrivs(ctx, res, postgres.Usage|postgres.Create)
	if err != nil {
		return nil, nil, err
	}

	return ens, &resource.SyncOpResults{}, nil
}

func (r *schemaSyncer) Grants(ctx context.Context, res *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	pToken := &opts.PageToken
	db, rID, err := parseWithDatabaseID(res.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	schema, err := client.GetSchema(ctx, rID)
	if err != nil {
		return nil, nil, err
	}

	roles, nextPageToken, err := client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, client, res, roles, schema)
	if err != nil {
		return nil, nil, err
	}

	return ret, &resource.SyncOpResults{NextPageToken: nextPageToken}, nil
}

func newSchemaSyncer(ctx context.Context, c *postgres.ClientDatabasesPool) *schemaSyncer {
	return &schemaSyncer{
		resourceType: schemaResourceType,
		clientPool:   c,
	}
}
