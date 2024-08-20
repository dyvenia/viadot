FROM python:3.10-slim-bullseye AS base

ARG PIP_NO_CACHE_DIR=1
ARG UV_NO_CACHE=true

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

# System packages
# unixodbc: used by pyodbc
# libgeos-dev: used by Shapely
# gcc: used by pandas
RUN apt update -q && yes | apt install -q gnupg vim curl git unixodbc libgeos-dev gcc

# Turn off package manager caches.
ENV PIP_NO_CACHE_DIR=${PIP_NO_CACHE_DIR}
ENV UV_NO_CACHE=${UV_NO_CACHE}

# This one's needed for the SAP RFC connector.
# It must be installed here as the `pyrfc` does not define its dependencies,
# so `pip install pyrfc` breaks if all deps are not already present.
RUN pip install cython==0.29.24

# Python env
RUN pip install --upgrade pip setuptools


ENV USER viadot
ENV HOME="/home/$USER"
ENV PATH="$HOME/.local/bin:$PATH"
ENV RYE_HOME="$HOME/.rye"
ENV PATH="$RYE_HOME/shims:$PATH"

# Install Rye and uv.
RUN curl -sSf https://rye.astral.sh/get | RYE_TOOLCHAIN_VERSION="3.10" RYE_INSTALL_OPTION="--yes" bash && \
    rye config --set-bool behavior.use-uv=true

# Fix for old SQL Servers still using TLS < 1.2
RUN chmod +rwx /usr/lib/ssl/openssl.cnf && \
    sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /usr/lib/ssl/openssl.cnf

# ODBC -- make sure to pin driver version as it's reflected in odbcinst.ini
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt update -q && \
    apt install -q libsqliteodbc && \
    ACCEPT_EULA=Y apt install -q -y msodbcsql17=17.10.1.1-1 && \
    ACCEPT_EULA=Y apt install -q -y mssql-tools=17.10.1.1-1 && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

COPY docker/odbcinst.ini /etc

WORKDIR ${HOME}

COPY --chown=${USER}:${USER} . ./viadot

###################
### viadot-lite ###
###################

FROM base as viadot-lite

# Turn off package manager caches.
ENV PIP_NO_CACHE_DIR=${PIP_NO_CACHE_DIR}
ENV UV_NO_CACHE=${UV_NO_CACHE}

RUN rye lock --reset --features viadot-lite --pyproject viadot/pyproject.toml
RUN sed '/-e/d' ./viadot/requirements.lock > ./viadot/requirements.txt
RUN pip install -r ./viadot/requirements.txt

RUN pip install ./viadot/

# Cleanup.
RUN rm -rf ./viadot

USER ${USER}


####################
### viadot-azure ###
####################

FROM base as viadot-azure

ARG INSTALL_DATABRICKS=false

# Databricks source setup
RUN if [ "$INSTALL_DATABRICKS" = "true" ]; then \
    apt-get update && \
    apt-get install -y wget apt-transport-https && \
    mkdir -p /etc/apt/keyrings && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-11-jdk && \
    find /usr/bin/java -type d -exec chmod 777 {} \; ; \
    fi

ENV SPARK_HOME /usr/local/lib/python3.10/site-packages/pyspark

ARG INSTALL_DATABRICKS=false

# Turn off package manager caches.
ENV PIP_NO_CACHE_DIR=${PIP_NO_CACHE_DIR}
ENV UV_NO_CACHE=${UV_NO_CACHE}

RUN rye lock --reset --features azure --pyproject viadot/pyproject.toml
RUN sed '/-e/d' ./viadot/requirements.lock > ./viadot/requirements.txt
RUN pip install -r ./viadot/requirements.txt

RUN if [ "$INSTALL_DATABRICKS" = "true" ]; then \
    pip install ./viadot/.[databricks]; \
    fi

# Dependecy install
RUN pip install ./viadot/.[azure]

# Cleanup.
RUN rm -rf ./viadot

USER ${USER}


##################
### viadot-aws ###
##################

FROM base as viadot-aws

# Turn off package manager caches.
ENV PIP_NO_CACHE_DIR=${PIP_NO_CACHE_DIR}
ENV UV_NO_CACHE=${UV_NO_CACHE}

RUN rye lock --reset --features aws --pyproject viadot/pyproject.toml
RUN sed '/-e/d' ./viadot/requirements.lock > ./viadot/requirements.txt
RUN pip install -r ./viadot/requirements.txt

RUN pip install ./viadot/.[aws]

# Cleanup.
RUN rm -rf ./viadot

USER ${USER}
