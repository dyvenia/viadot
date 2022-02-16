## SAP RFC example

This is an example environment for running the `SAPRFC` connector. 


Note that we refer to a `sap_netweaver_rfc` folder in the Dockerfile. This is the folder containing the proprietary SAP NetWeaver driver that would have to be obtained and installed by the user.

### Running SAPRFC
To build the image, run `docker build . -t viadot:sap_rfc`, and spin it up with the provided `docker-compose`: `docker-compose up -d`. You can now open up Jupyter Lab at `localhost:5678`. 

To run tests, run eg. `docker exec -it viadot_saprfc_lab pytest tests/integration/test_sap_rfc.py`.