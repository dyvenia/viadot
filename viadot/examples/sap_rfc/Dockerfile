FROM viadot:latest

USER root

COPY sap_netweaver_rfc/nwrfcsdk /usr/local/sap/nwrfcsdk
COPY sap_netweaver_rfc/nwrfcsdk.conf /etc/ld.so.conf.d/nwrfcsdk.conf

ENV SAPNWRFC_HOME=/usr/local/sap/nwrfcsdk

RUN ldconfig

RUN pip install pyrfc==2.5.0

USER viadot