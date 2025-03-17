package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

func (c *Postgresql) DeleteAccount(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	return nil, nil, fmt.Errorf("not implemented")
}
