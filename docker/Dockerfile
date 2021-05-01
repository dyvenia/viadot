FROM prefecthq/prefect:latest-python3.8


# Add user
RUN useradd --create-home viadot && \
    usermod -aG sudo viadot && \
    find /usr/local/lib -type d -exec chmod 777 {} \; && \
    find /usr/local/bin -type d -exec chmod 777 {} \;

RUN groupadd docker && \
    usermod -aG docker viadot

# Release File Error
# https://stackoverflow.com/questions/63526272/release-file-is-not-valid-yet-docker
RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until

# System packages
RUN apt-get update && yes | apt-get install vim gcc unixodbc-dev build-essential curl graphviz sudo

# Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# ODBC
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    apt-get install libsqliteodbc && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

COPY docker/odbcinst.ini /etc

# Python env
WORKDIR /code
COPY requirements.txt /code/
RUN pip install pip==20.0.2
RUN pip install -r requirements.txt


# Workdir
ENV USER viadot
ENV HOME="/home/$USER"
WORKDIR ${HOME}
USER ${USER}

EXPOSE 8000