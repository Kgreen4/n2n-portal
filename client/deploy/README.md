# Deploy

Local database container for the Svelte app collections.

```sh
docker compose -f client/deploy/docker/docker-compose.yml up --build
```

The local container is exposed on `localhost:5433` to avoid colliding with a system Postgres on `localhost:5432`.
The compose stack includes a `migrate` service that waits for Postgres and applies `client/deploy/migration/001_schema.sql` on startup.

The app reads database settings from:

```sh
client/deploy/configs/development.yaml
client/deploy/configs/production.yaml
```

The database initializes `patients` and `claims` from `client/deploy/migration/001_schema.sql`, and the migration service reapplies that idempotent schema when the stack starts.

If Postgres reports that role `n2n` does not exist, the app is usually connected to another local Postgres or an old Docker volume. Use the development DSN port `5433`, then recreate the local volume if needed.
