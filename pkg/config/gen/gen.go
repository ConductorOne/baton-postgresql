package main

import (
	cfg "github.com/conductorone/baton-postgresql/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/config"
)

func main() {
	config.Generate("postgresql", cfg.Config)
}
