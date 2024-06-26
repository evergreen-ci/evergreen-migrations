package migrations

import (
	"context"

	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

var Registry migrationRegistry

type migrationRegistry struct {
	migrations map[string]MigrationFactory
}

func (m *migrationRegistry) registerMigration(name string, factory MigrationFactory) {
	if m.migrations == nil {
		m.migrations = make(map[string]MigrationFactory)
	}
	m.migrations[name] = factory
}

func (m *migrationRegistry) Migration(name string, opts MigrationOptions) (Migration, error) {
	migrationFactory, ok := m.migrations[name]
	if !ok {
		return nil, errors.Errorf("no migration exists for ttlMigrationName '%s'", name)
	}
	return migrationFactory(opts)
}

type Migration interface {
	Execute(context.Context, *mongo.Client) error
}

type MigrationFactory func(MigrationOptions) (Migration, error)

type MigrationOptions struct {
	Database   string
	Collection string
	BatchSize  int
}

func (m *MigrationOptions) validate() error {
	catcher := grip.NewBasicCatcher()
	if m.Database == "" {
		catcher.Add(errors.New("database ttlMigrationName not specified"))
	}

	return catcher.Resolve()
}
