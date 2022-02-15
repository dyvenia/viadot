FROM viadot:latest

USER root

COPY sap_netweaver_rfc/nwrfcsdk /usr/local/sap/nwrfcsdk
COPY sap_netweaver_rfc/nwrfcsdk.conf /etc/ld.so.conf.d/nwrfcsdk.conf

ENV SAPNWRFC_HOME=/usr/local/sap/nwrfcsdk

RUN ldconfig

COPY requirements.txt .
RUN xargs -L 1 pip install < requirements.txt

USER viadot