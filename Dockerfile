FROM astrocrpublic.azurecr.io/runtime:3.3-2

# Nothing is copied here because the base image's ONBUILD steps already have.

# The loader runs as a DAG task rather than a shell step, so it has to import.
# Its groups come from pyproject.toml so the image and the Makefile cannot pin
# dbt differently; requirements.txt exists only to satisfy the ONBUILD above.
RUN pip install --no-cache-dir -e . --group dbt --group airflow

# Built here so Cosmos reads a manifest at scheduler-parse time rather than
# shelling out to `dbt ls`.
RUN dbt parse --project-dir transform --profiles-dir transform
