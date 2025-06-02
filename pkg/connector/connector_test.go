package connector

import (
	"context"
	"net"

	"github.com/conductorone/baton-postgresql/pkg/testutil"
	connectorV2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager/local"
	"github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"os"
	"testing"
)

const bufSize = 1024 * 1024

type inMemoryConnectorClient struct {
	connectorV2.ResourceTypesServiceClient
	connectorV2.ResourcesServiceClient
	connectorV2.ResourceGetterServiceClient
	connectorV2.EntitlementsServiceClient
	connectorV2.GrantsServiceClient
	connectorV2.ConnectorServiceClient
	connectorV2.AssetServiceClient
	connectorV2.GrantManagerServiceClient
	connectorV2.ResourceManagerServiceClient
	connectorV2.ResourceDeleterServiceClient
	connectorV2.AccountManagerServiceClient
	connectorV2.CredentialManagerServiceClient
	connectorV2.EventServiceClient
	connectorV2.TicketsServiceClient
	connectorV2.ActionServiceClient
}

func newTestConnector(t *testing.T) (context.Context, sync.Syncer, manager.Manager, *inMemoryConnectorClient) {
	ctx := context.Background()

	container := testutil.SetupPostgresContainer(ctx, t)

	postgresConnector, err := New(
		ctx,
		container.Dsn(),
		nil,
		true,
		true,
		true,
	)
	require.NoError(t, err)

	srv, err := connectorbuilder.NewConnector(ctx, postgresConnector)
	require.NoError(t, err)

	tempPath, err := os.CreateTemp("", "baton-postgresql-test-c1z")
	require.NoError(t, err)

	t.Cleanup(func() {
		err := os.Remove(tempPath.Name())
		require.NoError(t, err)
	})

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
		grpc.ChainUnaryInterceptor(ugrpc.UnaryServerInterceptor(ctx)...),
		grpc.ChainStreamInterceptor(ugrpc.StreamServerInterceptors(ctx)...),
		grpc.StatsHandler(
			otelgrpc.NewServerHandler(
				otelgrpc.WithPropagators(
					propagation.NewCompositeTextMapPropagator(
						propagation.TraceContext{},
						propagation.Baggage{},
					),
				),
			),
		),
	)

	connectorV2.RegisterConnectorServiceServer(s, srv)
	connectorV2.RegisterGrantsServiceServer(s, srv)
	connectorV2.RegisterEntitlementsServiceServer(s, srv)
	connectorV2.RegisterResourcesServiceServer(s, srv)
	connectorV2.RegisterResourceTypesServiceServer(s, srv)
	connectorV2.RegisterAssetServiceServer(s, srv)
	connectorV2.RegisterEventServiceServer(s, srv)
	connectorV2.RegisterResourceGetterServiceServer(s, srv)
	connectorV2.RegisterGrantManagerServiceServer(s, srv)
	connectorV2.RegisterResourceManagerServiceServer(s, srv)
	connectorV2.RegisterResourceDeleterServiceServer(s, srv)
	connectorV2.RegisterAccountManagerServiceServer(s, srv)
	connectorV2.RegisterCredentialManagerServiceServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Error("failed to serve:", err)
			return
		}
	}()

	t.Cleanup(func() {
		s.Stop()
	})

	bufDialer := func(ctx context.Context, s string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	cc, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := &inMemoryConnectorClient{
		ResourceTypesServiceClient:     connectorV2.NewResourceTypesServiceClient(cc),
		ResourcesServiceClient:         connectorV2.NewResourcesServiceClient(cc),
		EntitlementsServiceClient:      connectorV2.NewEntitlementsServiceClient(cc),
		GrantsServiceClient:            connectorV2.NewGrantsServiceClient(cc),
		ConnectorServiceClient:         connectorV2.NewConnectorServiceClient(cc),
		AssetServiceClient:             connectorV2.NewAssetServiceClient(cc),
		GrantManagerServiceClient:      connectorV2.NewGrantManagerServiceClient(cc),
		ResourceManagerServiceClient:   connectorV2.NewResourceManagerServiceClient(cc),
		ResourceDeleterServiceClient:   connectorV2.NewResourceDeleterServiceClient(cc),
		AccountManagerServiceClient:    connectorV2.NewAccountManagerServiceClient(cc),
		CredentialManagerServiceClient: connectorV2.NewCredentialManagerServiceClient(cc),
		EventServiceClient:             connectorV2.NewEventServiceClient(cc),
		TicketsServiceClient:           connectorV2.NewTicketsServiceClient(cc),
		ActionServiceClient:            connectorV2.NewActionServiceClient(cc),
		ResourceGetterServiceClient:    connectorV2.NewResourceGetterServiceClient(cc),
	}

	_, err = client.Validate(ctx, &connectorV2.ConnectorServiceValidateRequest{})
	require.NoError(t, err)

	syncer, err := sync.NewSyncer(
		ctx,
		client,
		sync.WithC1ZPath(tempPath.Name()),
	)
	require.NoError(t, err)

	localManager, err := local.New(ctx, tempPath.Name())
	require.NoError(t, err)

	return ctx, syncer, localManager, client
}

func getByDisplayName(ctx context.Context, c1z *dotc1z.C1File, resourceType *connectorV2.ResourceType, name string) (*connectorV2.Resource, error) {
	resources, err := c1z.ListResources(ctx, &connectorV2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: resourceType.Id,
		PageSize:       100,
	})
	if err != nil {
		return nil, err
	}

	for _, rs := range resources.List {
		if rs.DisplayName == name {
			return rs, nil
		}
	}

	return nil, nil
}

func TestConnectorFullSync(t *testing.T) {
	ctx, syncer, _, _ := newTestConnector(t)

	err := syncer.Sync(ctx)
	require.NoError(t, err)

	err = syncer.Close(ctx)
	require.NoError(t, err)
}
