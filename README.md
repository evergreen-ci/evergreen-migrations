Scripts to migrate data in Evergreen's database.

# Usage
Add your migration to the migrations directory. 
Invoke the migration from [Argo Workflows](https://kanopy.corp.mongodb.com/docs/beta/argo_workflows/).

# Migrations

## Parameters
Common options are set on the command line. These include
* `--url` (required): URL to connect to the database (e.g. mongodb://localhost:27017)
* `--db` (required): database your script will run against (e.g. mci)
* `--batch-size` (optional): number of documents to process at once
* `--collection` (optional): collection to run the script against

The values of these options are passed to your script in a `MigrationOptions`. Your script has the option to use the optional parameters when they are provided, but isn't required to. Since they're optional, it's advisable to default to something sensible when they aren't provided.

Any additional arguments your script requires can be passed through the environment.

Two additional parameters
* `--script` (required) the name of the script to run
* `--skip-db-auth` (optional) is used for testing against a local database

## Adding a script
Add a script to the migrations directory and register its factory
```go
func init() {
	Registry.registerMigration("cool-migration", NewCoolMigration)
}
```

### Expectations
* Because the script may be interrupted and restarted, your script should be idempotent
* The script must exit when it's complete

## Running a migration
### Local Testing
A migration can be tested locally with
```
go run migrator.go --url mongodb://localhost:27017 --db test_db --script cool-migration --skip-db-auth
```

### Atlas
Follow [the procedure in the Operations Guide](https://docs.google.com/document/d/14BTuPnzbSLCuewcMXFNQivkyUPy3Dsy1TYdF_9WVaBY/edit#heading=h.zh6mmdkbm119) to run a migration against the staging/production databases.
