package connector

import (
	"context"
	"fmt"
	"io"

	"github.com/conductorone/baton-postgresql/pkg/postgres"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
)

type Postgresql struct {
	client  *postgres.Client
	schemas []string
}

func (o *Postgresql) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		newRoleSyncer(ctx, o.client),
		newSchemaSyncer(ctx, o.client),
		newTableSyncer(ctx, o.client),
		newViewSyncer(ctx, o.client),
		newColumnSyncer(ctx, o.client),
		newFunctionSyncer(ctx, o.client),
		newProcedureSyncer(ctx, o.client),
		newLargeObjectSyncer(ctx, o.client),
		newDatabaseSyncer(ctx, o.client),
		newSequenceSyncer(ctx, o.client),
	}
}

func (c *Postgresql) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	var annos annotations.Annotations

	return &v2.ConnectorMetadata{
		DisplayName: "Postgresql",
		Annotations: annos,
	}, nil
}

func (c *Postgresql) Validate(ctx context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (c *Postgresql) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, fmt.Errorf("not implemented")
}

func New(ctx context.Context, dsn string, schemas []string) (*Postgresql, error) {
	c, err := postgres.New(ctx, dsn, postgres.WithSchemaFilter(schemas))
	if err != nil {
		return nil, err
	}
	return &Postgresql{
		client:  c,
		schemas: schemas,
	}, nil
}
