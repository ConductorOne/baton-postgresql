package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassifyConnectError(t *testing.T) {
	cases := []struct {
		name     string
		sqlstate string
		want     codes.Code
	}{
		{name: "invalid password", sqlstate: "28P01", want: codes.Unauthenticated},
		{name: "invalid authorization specification", sqlstate: "28000", want: codes.Unauthenticated},
		{name: "insufficient privilege", sqlstate: "42501", want: codes.PermissionDenied},
		{name: "unrelated server error", sqlstate: "3D000", want: codes.Unknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// pgx wraps the server error the same way ConnectConfig does.
			pgErr := &pgconn.PgError{Code: tc.sqlstate, Message: "server said no"}
			err := fmt.Errorf("failed to connect: %w", pgErr)

			got := classifyConnectError(err)

			require.Equal(t, tc.want, status.Code(got))
			var unwrapped *pgconn.PgError
			require.True(t, errors.As(got, &unwrapped), "original PgError must stay in the chain")
			require.Equal(t, tc.sqlstate, unwrapped.Code)
			require.Contains(t, got.Error(), "server said no")
		})
	}
}

func TestClassifyConnectErrorPassesThroughNonServerErrors(t *testing.T) {
	err := errors.New("dial tcp: connection refused")

	got := classifyConnectError(err)

	require.Same(t, err, got)
	require.Equal(t, codes.Unknown, status.Code(got))
}
