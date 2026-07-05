# Deploy

Local database container for the Svelte app collections.

```sh
docker compose -f client/deploy/docker/docker-compose.yml up --build
```

The app reads database settings from:

```sh
client/deploy/configs/development.yaml
client/deploy/configs/production.yaml
```

The database initializes `patients` and `claims` from `client/deploy/migration/001_schema.sql` on first volume creation.
