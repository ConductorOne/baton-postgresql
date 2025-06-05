package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	dsn = field.StringField(
		"dsn",
		field.WithDisplayName("DSN"),
		field.WithRequired(true),
		field.WithDescription("The DSN to connect to the database"),
		field.WithIsSecret(true),
	)
	schemas = field.StringSliceField(
		"schemas",
		field.WithDisplayName("Schemas"),
		field.WithDefaultValue([]string{"public"}),
		field.WithDescription("The schemas to include in the sync"),
	)
	includeColumns = field.BoolField(
		"include-columns",
		field.WithDisplayName("Include Columns"),
		field.WithDescription("Include column privileges when syncing. This can result in large amounts of data"),
	)
	includeLargeObjects = field.BoolField(
		"include-large-objects",
		field.WithDisplayName("Include Large Objects"),
		field.WithDescription("Include large objects when syncing. This can result in large amounts of data"),
	)
	syncAllDatabases = field.BoolField(
		"sync-all-databases",
		field.WithDisplayName("Sync All Databases"),
		field.WithDescription("Sync all databases. This can result in large amounts of data"),
		field.WithDefaultValue(false),
	)
)

//go:generate go run ./gen
var Config = field.NewConfiguration(
	[]field.SchemaField{
		dsn,
		schemas,
		includeColumns,
		includeLargeObjects,
		syncAllDatabases,
	},
	field.WithConnectorDisplayName("PostgreSQL"),
	field.WithHelpUrl("/docs/baton/postgresql"),
)
