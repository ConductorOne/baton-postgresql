package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

type SequenceModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"relname"`
	Schema  string   `db:"nspname"`
	OwnerID int64    `db:"relowner"`
	ACLs    []string `db:"relacl"`
}

func (t *SequenceModel) GetOwnerID() int64 {
	return t.OwnerID
}

func (t *SequenceModel) GetACLs() []string {
	return t.ACLs
}

func (t *SequenceModel) AllPrivileges() PrivilegeSet {
	return Select | Update | Usage
}

func (t *SequenceModel) DefaultPrivileges() PrivilegeSet {
	return EmptyPrivilegeSet
}

func (c *Client) getClassQuery(ctx context.Context) string {
	q := `
SELECT DISTINCT c."oid"::int, c."relname", c."relowner"::int, n."nspname", c."relacl"
FROM pg_class c
         LEFT JOIN pg_namespace n ON n."oid" = c."relnamespace"
WHERE c."oid" = $1
`
	return q
}

func (c *Client) GetSequence(ctx context.Context, sequenceID int64) (*SequenceModel, error) {
	ret := &SequenceModel{}

	q := c.getClassQuery(ctx)

	err := pgxscan.Get(ctx, c.db, ret, q, sequenceID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListSequences(ctx context.Context, schemaID int64, pager *Pager) ([]*SequenceModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing sequences")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	_, _ = sb.WriteString(`
SELECT c."oid"::int, c."relname", c."relowner"::int, n."nspname", c."relacl"
FROM pg_class c
         LEFT JOIN pg_namespace n ON n."oid" = c."relnamespace"
WHERE n."oid" = $1
  AND (c."relkind" = 'S')
`)

	args = append(args, schemaID)
	_, _ = sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		_, _ = sb.WriteString("OFFSET $3")
		args = append(args, offset)
	}

	var ret []*SequenceModel
	err = pgxscan.Select(ctx, c.db, &ret, sb.String(), args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", nil
		}
		return nil, "", err
	}

	var nextPageToken string
	if len(ret) > limit {
		offset += limit
		nextPageToken = strconv.Itoa(offset)
		ret = ret[:limit]
	}

	return ret, nextPageToken, nil
}

func (c *Client) GrantSequence(ctx context.Context, schema, sequenceName string, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("granting sequence", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedSequenceName := pgx.Identifier{sequenceName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := sanitizePrivilege(privilege)

	q := fmt.Sprintf("GRANT %s ON %s.%s TO %s", sanitizedPrivilege, sanitizedSchema, sanitizedSequenceName, sanitizedPrincipalName)

	if isGrant {
		q += withGrantOptions
	}

	_, err := c.db.Exec(ctx, q)
	return err
}

func (c *Client) RevokeSequence(ctx context.Context, schema, sequenceName string, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("revoking sequence", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedSequenceName := pgx.Identifier{sequenceName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := sanitizePrivilege(privilege)

	var q string

	if isGrant {
		q = fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON TABLE %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedSequenceName, sanitizedPrincipalName)
	} else {
		q = fmt.Sprintf("REVOKE %s ON TABLE %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedSequenceName, sanitizedPrincipalName)
	}

	_, err := c.db.Exec(ctx, q)
	return err
}
