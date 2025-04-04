package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	sdkResource "github.com/conductorone/baton-sdk/pkg/types/resource"
)

var roleResourceType = &v2.ResourceType{
	Id:          "role",
	DisplayName: "Role",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE, v2.ResourceType_TRAIT_USER},
	Annotations: nil,
}

type roleSyncer struct {
	resourceType *v2.ResourceType
	client       *postgres.Client
}

func (r *roleSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return roleResourceType
}

func (r *roleSyncer) makeResource(ctx context.Context, roleModel *postgres.RoleModel) (*v2.Resource, error) {
	var annos annotations.Annotations

	hasMembers, err := r.client.RoleHasMembers(ctx, roleModel.ID)
	if err != nil {
		return nil, err
	}

	if hasMembers {
		gt, err := sdkResource.NewGroupTrait()
		if err != nil {
			return nil, err
		}
		annos.Update(gt)
	}

	traitOptions := []sdkResource.UserTraitOption{
		sdkResource.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
	}

	switch {
	case roleModel.Name == "postgres":
		traitOptions = append(traitOptions, sdkResource.WithAccountType(v2.UserTrait_ACCOUNT_TYPE_SYSTEM))
	case roleModel.CanLogin:
		traitOptions = append(traitOptions, sdkResource.WithAccountType(v2.UserTrait_ACCOUNT_TYPE_HUMAN))
	default:
		traitOptions = append(traitOptions, sdkResource.WithAccountType(v2.UserTrait_ACCOUNT_TYPE_SERVICE))
	}

	traitOptions = append(traitOptions, sdkResource.WithUserLogin(roleModel.Name))
	parts := strings.SplitN(roleModel.Name, "@", 2)
	if len(parts) == 2 {
		traitOptions = append(traitOptions, sdkResource.WithEmail(roleModel.Name, true))
	}
	ut, err := sdkResource.NewUserTrait(traitOptions...)
	if err != nil {
		return nil, err
	}

	annos.Update(ut)
	rt, err := sdkResource.NewRoleTrait()
	if err != nil {
		return nil, err
	}
	annos.Update(rt)

	return &v2.Resource{
		DisplayName: roleModel.Name,
		Id: &v2.ResourceId{
			ResourceType: r.resourceType.Id,
			Resource:     formatObjectID(r.resourceType.Id, roleModel.ID),
		},
		Annotations: annos,
	}, nil
}

func (r *roleSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var err error

	// if we ever support parentResourceID, be sure to set it in makeResource
	if parentResourceID != nil {
		return nil, "", nil, fmt.Errorf("unexpected parent resource ID on role: %s", parentResourceID)
	}

	roles, nextPageToken, err := r.client.ListRoles(ctx, &postgres.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, o := range roles {
		resource, err := r.makeResource(ctx, o)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, resource)
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

func (r *roleSyncer) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	if resourceId.ResourceType != roleResourceType.Id {
		return nil, fmt.Errorf("baton-postgres: non-role/user resource passed to role delete")
	}

	roleId, err := parseObjectID(resourceId.Resource)
	if err != nil {
		return nil, err
	}

	pgRole, err := r.client.GetRole(ctx, roleId)
	if err != nil {
		return nil, err
	}

	err = r.client.DeleteRole(ctx, pgRole.Name)
	return nil, err
}

func (r *roleSyncer) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if principal.Id.ResourceType != roleResourceType.Id {
		return nil, nil, fmt.Errorf("baton-postgres: only users and roles can have roles granted")
	}

	// TODO: respect isGrant
	_, roleIdStr, _, _, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, nil, err
	}

	roleID, err := strconv.ParseInt(roleIdStr, 10, 64)
	if err != nil {
		return nil, nil, err
	}

	pgRole, err := r.client.GetRole(ctx, roleID)
	if err != nil {
		return nil, nil, err
	}

	principalId, err := parseObjectID(principal.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	pgPrincipal, err := r.client.GetRole(ctx, principalId)
	if err != nil {
		return nil, nil, err
	}
	// TODO: respect duration if it's provided
	err = r.client.GrantRole(ctx, pgRole.Name, pgPrincipal.Name)
	return nil, nil, err
}

func (r *roleSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	entitlement := grant.Entitlement
	principal := grant.Principal

	_, roleIdStr, _, isGrant, err := parseEntitlementID(entitlement.Id)
	if err != nil {
		return nil, err
	}

	roleID, err := strconv.ParseInt(roleIdStr, 10, 64)
	if err != nil {
		return nil, err
	}

	pgRole, err := r.client.GetRole(ctx, roleID)
	if err != nil {
		return nil, err
	}

	principalName := principal.DisplayName
	err = r.client.RevokeRole(ctx, pgRole.Name, principalName, isGrant)
	return nil, err
}

func (r *roleSyncer) RotateCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error) {
	return &v2.CredentialDetailsCredentialRotation{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD},
		PreferredCredentialOption:  v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
	}, nil, nil
}

func (r *roleSyncer) Rotate(
	ctx context.Context,
	resourceId *v2.ResourceId,
	credentialOptions *v2.CredentialOptions,
) (
	[]*v2.PlaintextData,
	annotations.Annotations,
	error,
) {
	if resourceId.ResourceType != roleResourceType.Id {
		return nil, nil, fmt.Errorf("baton-postgres: non-role/user resource passed to rotate credentials")
	}

	roleId, err := parseObjectID(resourceId.Resource)
	if err != nil {
		return nil, nil, err
	}

	pgRole, err := r.client.GetRole(ctx, roleId)
	if err != nil {
		return nil, nil, err
	}

	plainTextPassword, err := crypto.GeneratePassword(credentialOptions)
	if err != nil {
		return nil, nil, err
	}
	ptd := &v2.PlaintextData{
		Name:  "password",
		Bytes: []byte(plainTextPassword),
	}

	_, err = r.client.ChangePassword(ctx, pgRole.Name, plainTextPassword)
	if err != nil {
		return nil, nil, err
	}

	return []*v2.PlaintextData{ptd}, nil, nil
}

func (r *roleSyncer) CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return &v2.CredentialDetailsAccountProvisioning{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD},
		PreferredCredentialOption:  v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
	}, nil, nil
}

func (r *roleSyncer) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	credentialOptions *v2.CredentialOptions,
) (
	connectorbuilder.CreateAccountResponse,
	[]*v2.PlaintextData,
	annotations.Annotations,
	error,
) {
	roleModel, err := r.client.GetRoleByName(ctx, accountInfo.GetLogin())
	if err == nil {
		// user already exists. return that resource
		resource, err := r.makeResource(ctx, roleModel)
		if err != nil {
			return nil, nil, nil, err
		}
		car := &v2.CreateAccountResponse_SuccessResult{
			Resource: resource,
		}

		return car, []*v2.PlaintextData{}, nil, nil
	}

	plainTextPassword, err := crypto.GeneratePassword(credentialOptions)
	if err != nil {
		return nil, nil, nil, err
	}
	ptd := &v2.PlaintextData{
		Name:  "password",
		Bytes: []byte(plainTextPassword),
	}
	// Default to C1 User's login as email
	email := accountInfo.GetLogin()
	// If the account provisioning schema has been filled, use the calculated email field
	if accountInfo.Profile != nil {
		profileMap := accountInfo.Profile.GetFields()
		if value, ok := profileMap["email"]; ok && value.GetStringValue() != "" {
			email = value.GetStringValue()
		}
	}
	roleModel, err = r.client.CreateUser(ctx, email, plainTextPassword)
	if err != nil {
		return nil, nil, nil, err
	}

	resource, err := r.makeResource(ctx, roleModel)
	if err != nil {
		return nil, nil, nil, err
	}

	car := &v2.CreateAccountResponse_SuccessResult{
		Resource: resource,
	}

	return car, []*v2.PlaintextData{ptd}, nil, nil
}

func newRoleSyncer(ctx context.Context, c *postgres.Client) *roleSyncer {
	return &roleSyncer{
		resourceType: roleResourceType,
		client:       c,
	}
}
