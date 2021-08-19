FROM prefecthq/prefect:latest-python3.8


# Add user
RUN useradd --create-home viadot && \
    chown -R viadot /home/viadot && \
    usermod -aG sudo viadot && \
    find /usr/local/lib -type d -exec chmod 777 {} \; && \
    find /usr/local/bin -type d -exec chmod 777 {} \;

RUN groupadd docker && \
    usermod -aG docker viadot

# Release File Error
# https://stackoverflow.com/questions/63526272/release-file-is-not-valid-yet-docker
RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until

# System packages
RUN apt update && yes | apt install vim unixodbc-dev build-essential \
    curl python3-dev libboost-all-dev libpq-dev graphviz python3-gi sudo git
RUN pip install --upgrade cffi

RUN curl http://archive.ubuntu.com/ubuntu/pool/main/g/glibc/multiarch-support_2.27-3ubuntu1_amd64.deb \
    -o multiarch-support_2.27-3ubuntu1_amd64.deb && \
    apt install ./multiarch-support_2.27-3ubuntu1_amd64.deb


# ODBC -- make sure to pin driver version as it's reflected in odbcinst.ini
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt update && \
    apt install libsqliteodbc && \
    ACCEPT_EULA=Y apt install -y msodbcsql17=17.8.1.1-1 && \
    ACCEPT_EULA=Y apt install -y mssql-tools && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

COPY docker/odbcinst.ini /etc

# Python env
WORKDIR /code
COPY requirements.txt /code/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .
RUN pip install .

RUN rm -rf /code


# Workdir
ENV USER viadot
ENV HOME="/home/$USER"
WORKDIR ${HOME}

USER ${USER}



EXPOSE 8000