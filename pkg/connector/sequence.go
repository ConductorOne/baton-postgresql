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
	clientPool   *postgres.ClientDatabasesPool
}

func (r *sequenceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return sequenceResourceType
}

func (r *sequenceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	if parentResourceID == nil || pToken == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource ID on sequence")
	}

	db, parentID, err := parseWithDatabaseID(parentResourceID.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, "", nil, err
	}

	sequences, nextPageToken, err := client.ListSequences(ctx, parentID, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
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
				Resource:     formatWithDatabaseID(sequenceResourceType.Id, db, o.ID),
			},
			ParentResourceId: parentResourceID,
			Annotations:      annos,
		})
	}

	return ret, nextPageToken, nil, nil
}

func (r *sequenceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	dbId, _, err := parseWithDatabaseID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	dbModel, err := r.clientPool.
		Default(ctx).
		GetDatabaseById(ctx, dbId)

	if err != nil {
		return nil, "", nil, err
	}

	ens, err := entitlementsForPrivs(
		ctx,
		resource,
		postgres.Select|postgres.Update|postgres.Usage,
	)
	if err != nil {
		return nil, "", nil, err
	}

	for _, en := range ens {
		en.DisplayName = fmt.Sprintf("%s on %s", dbModel.Name, en.DisplayName)
	}

	return ens, "", nil, nil
}

func (r *sequenceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	db, rID, err := parseWithDatabaseID(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, err
	}

	client, _, err := r.clientPool.Get(ctx, db)
	if err != nil {
		return nil, "", nil, err
	}

	sequence, err := client.GetSequence(ctx, rID)
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	ret, err := roleGrantsForPrivileges(ctx, client, resource, roles, sequence)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, nextPageToken, nil, nil
}

func (r *sequenceSyncer) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, nil, fmt.Errorf("baton-postgres: only users and roles can have sequence granted")
	}

	_, _, privilegeName, isGrant, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, nil, err
	}

	dbId, rID, err := parseWithDatabaseID(entitlement.Resource.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	dbClient, _, err := r.clientPool.Get(ctx, dbId)
	if err != nil {
		return nil, nil, err
	}

	sequence, err := dbClient.GetSequence(ctx, rID)
	if err != nil {
		return nil, nil, err
	}

	err = dbClient.GrantSequence(ctx, sequence.Schema, sequence.Name, principal.DisplayName, privilegeName, isGrant)
	if err != nil {
		return nil, nil, err
	}

	return []*v2.Grant{
		{
			Id:          fmt.Sprintf("%s:%s:%s", entitlement.Id, principal.Id.ResourceType, principal.Id.Resource),
			Entitlement: entitlement,
			Principal:   principal,
		},
	}, nil, nil
}

func (r *sequenceSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	entitlement := grant.Entitlement
	principal := grant.Principal

	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, fmt.Errorf("baton-postgres: only users and roles can have sequence revoked")
	}

	_, _, privilegeName, isGrant, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, err
	}

	dbId, rID, err := parseWithDatabaseID(entitlement.Resource.Id.Resource)
	if err != nil {
		return nil, err
	}

	dbClient, _, err := r.clientPool.Get(ctx, dbId)
	if err != nil {
		return nil, err
	}

	sequence, err := dbClient.GetSequence(ctx, rID)
	if err != nil {
		return nil, err
	}

	err = dbClient.RevokeSequence(ctx, sequence.Schema, sequence.Name, principal.DisplayName, privilegeName, isGrant)
	return nil, err
}

func newSequenceSyncer(ctx context.Context, c *postgres.ClientDatabasesPool) *sequenceSyncer {
	return &sequenceSyncer{
		resourceType: sequenceResourceType,
		clientPool:   c,
	}
}
