![Baton Logo](./docs/images/baton-logo.png)

#

`baton-postgresql` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-postgresql.svg)](https://pkg.go.dev/github.com/conductorone/baton-postgresql) ![main ci](https://github.com/conductorone/baton-postgresql/actions/workflows/main.yaml/badge.svg)

`baton-postgresql` is a connector for PostgreSQL built using the [Baton SDK](https://github.com/conductorone/baton-sdk).
It connects to a PostgreSQL database and syncs data about which roles have access to which resources within the
database.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Getting Started

Needs postgresql DSN, if no database is selected in the DSN, it will use all databases.

## Troubleshooting

If you are having issues with the connector, please check the following:

- Specified database
  - User needs permission to access the database, this will cause missing resources.
- Not specified database
  - User needs permission for each database, otherwise it will not be able to read the resources for that database.

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-postgresql

baton-postgresql --dsn "postgres://username:password@localhost:5432/database_name"
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_DSN=postgres://username:password@localhost:5432/database_name ghcr.io/conductorone/baton-postgresql:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-postgresql/cmd/baton-postgresql@main

baton-postgresql --dsn "postgres://username:password@localhost:5432/database_name"
baton resources
```

# Data Model

`baton-postgresql` will sync information about the following PostgreSQL resources:

- Roles
- Databases
- Schemas
- Functions/Procedures
- Tables/Views
- Sequences
- Columns
- Large Objects

By default, `baton-postgresql` will only sync information from the `public` schema. You can use the `--schemas` flag to
specify other schemas.

# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome
contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for
everyone. If you have questions, problems, or ideas: Please open a Github Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-postgresql` Command Line Usage

```
baton-postgresql

Usage:
  baton-postgresql [flags]
  baton-postgresql [command]

Available Commands:
  capabilities       Get connector capabilities
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
      --client-id string        The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string    The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
      --dsn string              required: The DSN to connect to the database ($BATON_DSN)
  -f, --file string             The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                    help for baton-postgresql
      --include-columns         Include column privileges when syncing. This can result in large amounts of data ($BATON_INCLUDE_COLUMNS)
      --include-large-objects   Include large objects when syncing. This can result in large amounts of data ($BATON_INCLUDE_LARGE_OBJECTS)
      --log-format string       The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string        The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
  -p, --provisioning            This must be set in order for provisioning actions to be enabled ($BATON_PROVISIONING)
      --schemas strings         The schemas to include in the sync ($BATON_SCHEMAS) (default [public])
      --skip-full-sync          This must be set to skip a full sync ($BATON_SKIP_FULL_SYNC)
      --ticketing               This must be set to enable ticketing support ($BATON_TICKETING)
  -v, --version                 version for baton-postgresql

Use "baton-postgresql [command] --help" for more information about a command.
```
