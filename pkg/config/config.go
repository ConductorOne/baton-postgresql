package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	dsn                  = field.StringField("dsn", field.WithRequired(true), field.WithDescription("The DSN to connect to the database"))
	schemas              = field.StringSliceField("schemas", field.WithDefaultValue([]string{"public"}), field.WithDescription("The schemas to include in the sync"))
	includeColumns       = field.BoolField("include-columns", field.WithDescription("Include column privileges when syncing. This can result in large amounts of data"))
	includeLargeObjects  = field.BoolField("include-large-objects", field.WithDescription("Include large objects when syncing. This can result in large amounts of data"))
	syncAllDatabases     = field.BoolField("sync-all-databases", field.WithDescription("Sync all databases. This can result in large amounts of data"), field.WithDefaultValue(false))
	skipBuiltInFunctions = field.BoolField("skip-built-in-functions", field.WithDescription("Skip postgres built in functions"), field.WithDefaultValue(false))
)

var relationships = []field.SchemaFieldRelationship{}

//go:generate go run ./gen
var Config = field.NewConfiguration([]field.SchemaField{
	dsn, schemas, includeColumns, includeLargeObjects, syncAllDatabases, skipBuiltInFunctions,
}, relationships...)
