FROM prefecthq/prefect:2.0b6-python3.10

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
RUN apt update -q && yes | apt install -q vim unixodbc-dev build-essential \
    curl python3-dev libboost-all-dev libpq-dev graphviz python3-gi sudo git software-properties-common
RUN pip install --upgrade cffi

RUN curl http://archive.ubuntu.com/ubuntu/pool/main/g/glibc/multiarch-support_2.27-3ubuntu1_amd64.deb \
    -o multiarch-support_2.27-3ubuntu1_amd64.deb && \
    apt install -q ./multiarch-support_2.27-3ubuntu1_amd64.deb

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
RUN pip install cython==0.29.24

# Python env
WORKDIR /code
COPY requirements.txt /code/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .
RUN pip install .

## Install Java 11
RUN curl https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
    add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
    apt update -q && \
    apt install -q adoptopenjdk-11-hotspot -y && \
    find /usr/bin/java -type d -exec chmod 777 {} \;

### Export env variable
ENV SPARK_HOME /usr/local/lib/python3.9/site-packages/pyspark
RUN export SPARK_HOME

RUN rm -rf /code

# Workdir
ENV USER viadot
ENV HOME="/home/$USER"

WORKDIR ${HOME}

USER ${USER}

EXPOSE 8000
