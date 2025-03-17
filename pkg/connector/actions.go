package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/structpb"
)

func (c *Postgresql) DeleteAccount(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	// need to parse the rid
	fields := args.GetFields()
	rawRid, ok := fields["rid"]
	if !ok {
		return nil, nil, fmt.Errorf("rid is required")
	}
	strRid := rawRid.GetStringValue()
	if strRid == "" {
		return nil, nil, fmt.Errorf("rid is required")
	}

	var rid v2.ResourceId
	err := prototext.Unmarshal([]byte(strRid), &rid)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rid: %w", err)
	}

	if rid.ResourceType != roleResourceType.Id {
		return nil, nil, fmt.Errorf("rid is not a role")
	}

	roleId, err := parseObjectID(rid.Resource)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rid: %w", err)
	}

	pgRole, err := c.client.GetRole(ctx, roleId)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rid: %w", err)
	}

	err = c.client.DeleteRole(ctx, pgRole.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rid: %w", err)
	}

	return nil, nil, nil
}
