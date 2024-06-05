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

# System packages
RUN apt update -q && yes | apt install -q  gnupg vim curl git
ENV PIP_NO_CACHE_DIR=1

COPY docker/odbcinst.ini /etc

# This one's needed for the SAP RFC connector.
# It must be installed here as the SAP package does not define its dependencies,
# so `pip install pyrfc` breaks if all deps are not already present.
RUN pip install cython==0.29.24

# Python env
RUN pip install --upgrade pip

ENV USER viadot
ENV HOME="/home/$USER"
ENV PATH="$HOME/.local/bin:$PATH"

WORKDIR ${HOME}

COPY --chown=${USER}:${USER} . ./viadot

COPY requirements.lock ./viadot
RUN sed '/-e/d' ./viadot/requirements.lock > ./viadot/requirements.txt
RUN pip install --no-cache-dir -r ./viadot/requirements.txt

# Dependecy install
RUN pip install ./viadot/.[viadot-aws]

# Cleanup.
RUN rm -rf ./viadot

USER ${USER}
