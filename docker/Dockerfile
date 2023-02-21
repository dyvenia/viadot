FROM python:3.10-slim-bullseye

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
RUN apt update -q && yes | apt install -q gnupg vim unixodbc-dev build-essential \
    curl python3-dev libboost-all-dev libpq-dev python3-gi sudo git software-properties-common
ENV PIP_NO_CACHE_DIR=1
RUN pip install --upgrade cffi

# Fix for old SQL Servers still using TLS < 1.2
RUN chmod +rwx /usr/lib/ssl/openssl.cnf && \
    sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /usr/lib/ssl/openssl.cnf

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

# BEGIN Databricks source setup
RUN curl https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
    add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
    apt update -q && \
    apt install -q adoptopenjdk-11-hotspot -y && \
    find /usr/bin/java -type d -exec chmod 777 {} \;

ENV SPARK_HOME /usr/local/lib/python3.10/site-packages/pyspark
RUN export SPARK_HOME
# END Databricks source setup

RUN rm -rf /code

# Workdir
ENV USER viadot
ENV HOME="/home/$USER"

WORKDIR ${HOME}

USER ${USER}
