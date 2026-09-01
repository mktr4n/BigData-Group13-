FROM quay.io/jupyter/pyspark-notebook:latest

# The base image chain (pyspark-notebook -> scipy-notebook) already ships
# PySpark, pandas, pyarrow, scikit-learn, scipy and matplotlib.
# Only these two are missing:
#   pymongo  - direct MongoDB access from notebooks
#   requests - REST calls against the Regnskapsregisteret API
RUN pip install --no-cache-dir \
        pymongo \
        requests

# Static Jupyter token so the URL is stable and login is non-interactive:
#   http://localhost:8889/lab?token=group13
RUN jupyter server --generate-config -y && \
    echo "c.ServerApp.token = 'group13'" >> ~/.jupyter/jupyter_server_config.py