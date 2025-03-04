#!/bin/bash

# Exit on error
set -e

# Database configuration
DB_NAME="entities"
DB_USER="postgres"
DB_PASSWORD="password"
DB_HOST="localhost"
DB_PORT="5434"

# Get the script's directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Function to check if PostgreSQL is ready
wait_for_postgres() {
    echo "Waiting for PostgreSQL to be ready..."
    for i in {1..30}; do
        if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -p $DB_PORT -tc "SELECT 1;" postgres >/dev/null 2>&1; then
            echo "PostgreSQL is ready!"
            return 0
        fi
        echo "Attempt $i: PostgreSQL is not ready yet..."
        sleep 2
    done
    echo "Failed to connect to PostgreSQL after 30 attempts"
    return 1
}

# Function to create database if it doesn't exist
create_database() {
    echo "Checking if database $DB_NAME exists..."
    if ! PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -p $DB_PORT -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME';" postgres | grep -q 1; then
        echo "Creating database $DB_NAME..."
        PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -p $DB_PORT -c "CREATE DATABASE $DB_NAME;"
        echo "Database created successfully."
    else
        echo "Database $DB_NAME already exists."
    fi
}

# Function to apply schema
apply_schema() {
    echo "Applying database schema..."
    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -p $DB_PORT -d $DB_NAME -f "$PROJECT_ROOT/app/db/init.sql"
    echo "Database schema applied successfully."
}

# Main execution
echo "Starting database setup..."

# Wait for PostgreSQL to be ready
wait_for_postgres || exit 1

# Create database if it doesn't exist
create_database

# Apply schema
apply_schema

echo "Database setup completed successfully!"
