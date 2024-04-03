FROM python:3.10-slim-bullseye

# Add user
RUN useradd --non-unique --uid 1000 --create-home viadot && \
    chown -R viadot /home/viadot && \
    usermod -aG sudo viadot && \
    find /usr/local/lib -type d -exec chmod 777 {} \; && \
    find /usr/local/bin -type d -exec chmod 777 {} \;

# For easy removal of dotfiles. Must be executed with bash.
SHELL ["/bin/bash", "-c"]
RUN shopt -s dotglob
SHELL ["/bin/sh", "-c"]

RUN groupadd docker && \
    usermod -aG docker viadot

# Release File Error
# https://stackoverflow.com/questions/63526272/release-file-is-not-valid-yet-docker
RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until

# System packages
RUN apt update -q && yes | apt install -q gnupg vim unixodbc-dev build-essential \
    curl python3-dev libboost-all-dev libpq-dev python3-gi sudo git software-properties-common
ENV PIP_NO_CACHE_DIR=1
RUN pip install --upgrade cffi

# Fix for old SQL Servers still using TLS < 1.2
RUN chmod +rwx /usr/lib/ssl/openssl.cnf && \
    sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /usr/lib/ssl/openssl.cnf

# ODBC -- make sure to pin driver version as it's reflected in odbcinst.ini
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt update -q && \
    apt install -q libsqliteodbc && \
    ACCEPT_EULA=Y apt install -q -y msodbcsql17=17.8.1.1-1 && \
    ACCEPT_EULA=Y apt install -q -y mssql-tools && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

COPY docker/odbcinst.ini /etc

# This one's needed for the SAP RFC connector.
# It must be installed here as the SAP package does not define its dependencies,
# so `pip install pyrfc` breaks if all deps are not already present.
RUN pip install cython==0.29.24 --user

ARG databricks

# BEGIN Databricks source setup
RUN if [ "$databricks" = "true" ]; then \
    apt-get update && \
    apt-get install -y wget apt-transport-https && \
    mkdir -p /etc/apt/keyrings && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-11-jdk && \
    find /usr/bin/java -type d -exec chmod 777 {} \; && \
    pip install .[databricks] --user; \
    fi

ENV SPARK_HOME /usr/local/lib/python3.10/site-packages/pyspark
# END Databricks source setup

# Python env
RUN pip install --upgrade pip

ENV USER viadot
USER ${USER}
ENV HOME="/home/$USER"
ENV PATH="$HOME/.local/bin:$PATH"

WORKDIR ${HOME}

COPY --chown=${USER}:${USER} . ./viadot

RUN pip install ./viadot --user

# Cleanup.
RUN rm -rf ./viadot
