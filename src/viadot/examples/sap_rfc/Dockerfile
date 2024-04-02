FROM ghcr.io/dyvenia/viadot/viadot:2.0-latest

USER root

COPY sap_netweaver_rfc/nwrfcsdk /usr/local/sap/nwrfcsdk
COPY sap_netweaver_rfc/nwrfcsdk.conf /etc/ld.so.conf.d/nwrfcsdk.conf

ENV SAPNWRFC_HOME=/usr/local/sap/nwrfcsdk

RUN ldconfig

COPY requirements.txt .
RUN xargs -L 1 pip install < requirements.txt

ARG HTTP_PROXY=""
ARG NO_PROXY=""
ENV HTTP_PROXY=$HTTP_PROXY
ENV NO_PROXY=$NO_PROXY
RUN git config --global http.proxy ${HTTP_PROXY:-""}

USER viadot
