#!/bin/bash

function runDbMigrations() {
    echo "Migrating up $POSTGRES_DB"

    psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/postgres \
        -tAc "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB'" | grep -q 1 || \
    psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/postgres \
        -c "CREATE DATABASE $POSTGRES_DB"

    liquibase --driver=org.postgresql.Driver \
        --changeLogFile="changelog/db.changelog.xml" \
        --searchPath="/liquibase" \
        --url=jdbc:postgresql://$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB \
        --username="$POSTGRES_USER" --password="$POSTGRES_PASSWORD" --defaultSchemaName="public" \
        update
}

runDbMigrations
