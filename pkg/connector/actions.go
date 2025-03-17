package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

func (c *Postgresql) DeleteAccount(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
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

	parts := strings.SplitN(strRid, ":", 4)
	if len(parts) != 4 {
		return nil, nil, fmt.Errorf("invalid rid: %s", strRid)
	}

	rid, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rid: %w", err)
	}

	l.Info("deleting account", zap.String("rid", strRid))

	pgRole, err := c.client.GetRole(ctx, rid)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rid: %w", err)
	}

	err = c.client.DeleteRole(ctx, pgRole.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rid: %w", err)
	}

	return nil, nil, nil
}
