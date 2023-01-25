## SAP RFC example

This is an example environment for running the `SAPRFC` connector. 


Note that we refer to a `sap_netweaver_rfc` folder in the Dockerfile. This is the folder containing the proprietary SAP NetWeaver driver that would have to be obtained and installed by the user.

## Running SAPRFC
Clone the viadot, enter the sap_rfc folder, and build the image:
```
git clone https://github.com/dyvenia/viadot.git && \
cd viadot/viadot/examples/sap_rfc && \
docker build -t viadot:sap_rfc . --no-cache
```

Spin it up with the provided `docker-compose`
```
docker-compose up -d
```

You can now open up Jupyter Lab at `localhost:5678`. 

## Config File
Credentials and other settings are stored in a file named `credentials.json`. A credential file needs to be written in json format. A typical credentials file looks like so:

```
{
    "SAP": {
        "PROD": {
            "sysnr": "system_number_prod",
            "user": "user_name_prod",
            "passwd": "password_prod",
            "ashost": "host_name_prod"
        },
        "DEV": {
            "sysnr": "system_number_dev",
            "user": "user_name_dev",
            "passwd": "password_dev",
            "ashost": "host_name_dev"
        }
    }
}
```

## Running tests
To run tests, run pytest:
```
docker exec -it viadot_saprfc_lab pytest tests/integration/test_sap_rfc.py
```