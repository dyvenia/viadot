FROM viadot:dev

USER root

COPY sap_netweaver_rfc/nwrfcsdk /usr/local/sap/nwrfcsdk
COPY sap_netweaver_rfc/nwrfcsdk.conf /etc/ld.so.conf.d/nwrfcsdk.conf

ENV SAPNWRFC_HOME=/usr/local/sap/nwrfcsdk

RUN ldconfig

USER ${USER}

ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""
ARG NO_PROXY=""
ENV HTTP_PROXY=$HTTP_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
ENV NO_PROXY=$NO_PROXY
RUN git config --global http.proxy ${HTTP_PROXY:-""}

RUN pip install pyrfc==2.5.0

ARG VIADOT_USER=viadot_user
ARG GID=1111
ARG UID=1111
RUN groupadd -g $GID -o $VIADOT_USER
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $VIADOT_USER

USER $VIADOT_USER
