FROM ghcr.io/dbt-labs/dbt-postgres:1.7.4

WORKDIR /usr/app/dbt_project

# Set environment variables
ENV DBT_PROFILES_DIR=. \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 1. Copy only the files needed for dbt deps (if any were used, though we use pip packages)
# For dbt, we typically copy the whole project as it's small, 
# but we can still separate profiles and project config for consistency.
COPY dbt_project/dbt_project.yml .
COPY dbt_project/profiles.yml .

# 2. Copy the rest of the dbt models and configurations
COPY dbt_project/ .

# Default command: run dbt models every N seconds
ENTRYPOINT []
CMD ["sh", "-c", "while true; do dbt run && dbt test; sleep ${DBT_RUN_INTERVAL_SECONDS:-300}; done"]
