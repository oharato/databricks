FROM mcr.microsoft.com/devcontainers/python:2-3.14-trixie

# Keep apt in a valid state even if the base image includes the Yarn repo.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends just \
    && rm -rf /var/lib/apt/lists/*

COPY ../stock_analysis_app/requirements.txt /tmp/requirements.txt

USER vscode
RUN pip3 install --user --no-cache-dir -r /tmp/requirements.txt
