# Airflow image for orchestrating the pipeline locally.
#
# The pipeline's own dependencies (PySpark, Delta Lake, Great Expectations,
# clickhouse-connect) are installed into a SEPARATE virtualenv
# (/opt/pipeline-venv) rather than into Airflow's own Python environment.
# Great Expectations in particular pulls in a large, fast-moving dependency
# tree (pydantic, sqlalchemy, marshmallow, ...) that would be very likely to
# collide with Airflow's own pinned constraints if installed side by side.
# DAG tasks shell out to this venv's interpreter instead of importing the
# pipeline package directly into the Airflow webserver/scheduler process.
FROM apache/airflow:2.10.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY pyproject.toml /opt/project/pyproject.toml
COPY src /opt/project/src

# A pip cache mount (persists across separate build invocations, unlike a
# layer cache) so an edit under src/ doesn't force a full re-download of
# PySpark and friends every time — the venv itself is still built fresh.
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m venv /opt/pipeline-venv \
    && /opt/pipeline-venv/bin/pip install --upgrade pip \
    && /opt/pipeline-venv/bin/pip install -e /opt/project \
    && chown -R airflow:root /opt/pipeline-venv /opt/project

ENV PIPELINE_PYTHON=/opt/pipeline-venv/bin/python

# /opt/airflow/warehouse is mounted as an empty named volume by
# docker-compose.yml. Docker seeds a fresh named volume from whatever
# already exists at that path in the image, ownership included — so this
# has to be created (and owned by airflow) at build time, or the volume
# ends up root-owned and every Spark write into it fails.
RUN mkdir -p /opt/airflow/warehouse && chown airflow:root /opt/airflow/warehouse

USER airflow
