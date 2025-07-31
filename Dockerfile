FROM astrocrpublic.azurecr.io/runtime:3.0-6

# install dbt into a virtual environment
RUN python -m venv .venv && source .venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate
