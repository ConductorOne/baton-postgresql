package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var functionResourceType = &v2.ResourceType{
	Id:          "function",
	DisplayName: "Function",
	Traits:      nil,
	Annotations: nil,
}

type functionSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *functionSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return functionResourceType
}

func (r *functionSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on function")
	}

	parentID, err := parseObjectID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	functions, nextPageToken, err := r.client.ListFunctions(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range functions {
		var annos annotations.Annotations

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatObjectID(functionResourceType.Id, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *functionSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(ctx, resource, postgres.Execute)
	if err != nil {
		return nil, "", nil, err
	}

	return ens, "", nil, nil
}

func (r *functionSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	rID, err := parseObjectID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	function, err := r.client.GetFunction(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := grantsForPrivs(ctx, resource, r.client, function.ACLs, postgres.Execute)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, "", nil, nil
}

func newFunctionSyncer(ctx context.Context, c *postgres.Client) *functionSyncer {
	return &functionSyncer{
		resourceType: functionResourceType,
		client:       c,
	}
}
