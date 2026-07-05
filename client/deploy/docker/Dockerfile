FROM postgres:16-alpine

LABEL org.opencontainers.image.title="n2n-portal-postgres"
LABEL org.opencontainers.image.description="Local Postgres database for N2N Portal patient and claim collections"

COPY migration/*.sql /docker-entrypoint-initdb.d/
