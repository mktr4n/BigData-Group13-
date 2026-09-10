# Pinned by digest, not :latest. The report quotes exact versions (Spark 4.2.0,
# Python 3.13.15) and the benchmark timings are only meaningful against the
# engine that produced them; a rebuild months later against :latest would
# silently reproduce neither. To move deliberately, pull the new tag, read its
# digest with `docker image inspect --format "{{.RepoDigests}}"`, and update it
# here.
# quay.io/jupyter/pyspark-notebook:latest as of 2026-08-24 - Spark 4.2.0, Python 3.13.15
FROM quay.io/jupyter/pyspark-notebook@sha256:c6040f714d41191d1946fc3c438ee0282961b50172079f734b76b53741ae8c1c

# The base image chain (pyspark-notebook -> scipy-notebook) already ships
# PySpark, pandas, pyarrow, scikit-learn, scipy and matplotlib.
# Only these two are missing:
#   pymongo  - direct MongoDB access from notebooks
#   requests - REST calls against the Regnskapsregisteret API
RUN pip install --no-cache-dir \
        pymongo \
        requests

USER root
 
# Spark settings live in a config file rather than in notebook code, so that
# every session gets an identically configured JVM. 

COPY spark-defaults.conf /tmp/spark-defaults.conf
RUN cp /tmp/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf" && \
    chmod 0644 "${SPARK_HOME}/conf/spark-defaults.conf" && \
    rm /tmp/spark-defaults.conf
 
USER ${NB_UID}

# Static Jupyter token so the URL is stable and login is non-interactive:
#   http://localhost:8889/lab?token=group13
RUN jupyter server --generate-config -y && \
    echo "c.ServerApp.token = 'group13'" >> ~/.jupyter/jupyter_server_config.py