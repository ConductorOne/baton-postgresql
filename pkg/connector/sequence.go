package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

var sequenceResourceType = &v2.ResourceType{
	Id:          "sequence",
	DisplayName: "Sequence",
	Traits:      nil,
	Annotations: nil,
}

type sequenceSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *sequenceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return sequenceResourceType
}

func (r *sequenceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on sequence")
	}

	parentID, err := parseObjectID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	sequences, nextPageToken, err := r.client.ListSequences(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range sequences {
		var annos annotations.Annotations

		ret = append(ret, &v2.Resource{
			DisplayName: o.Name,
			Id: &v2.ResourceId{
				ResourceType: r.resourceType.Id,
				Resource:     formatObjectID(sequenceResourceType.Id, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *sequenceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	ens, err := entitlementsForPrivs(
		ctx,
		resource,
		postgres.Select|postgres.Update|postgres.Usage,
	)
	if err != nil {
		return nil, "", nil, err
	}

	return ens, "", nil, nil
}

func (r *sequenceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	rID, err := parseObjectID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	sequence, err := r.client.GetSequence(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := grantsForPrivs(
		ctx,
		resource,
		r.client,
		sequence.ACLs,
		postgres.Select|postgres.Update|postgres.Usage,
	)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, "", nil, nil
}

func newSequenceSyncer(ctx context.Context, c *postgres.Client) *sequenceSyncer {
	return &sequenceSyncer{
		resourceType: sequenceResourceType,
		client:       c,
	}
}
