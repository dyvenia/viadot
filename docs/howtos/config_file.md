# Config File

Credentials and other settings for various **sources** are stored in a file named `credentials.json`. A credential file needs to be written in json format. A typical credentials file looks like so:

```json
{
    "SUPERMETRICS": {
        "API_KEY": "apikey from supermetrics",
        "USER": "user@gmail.com",
        "SOURCES": {
            "Google Ads": {
                "Accounts": [
                    "456"
                ]
            }
        }
    },
    "AZURE_SQL": {
        "server": "server url",
        "db_name": "db name",
        "user": "user",
        "password": "db password",
        "driver": "driver"
    }
}
```

In the above **SUPERMETRICS** and **AZURE_SQL** are config_keys. These config settings are fed to the `Supermetrics()` or `AzureSQL()` Sources.

For example, this is how to use the **AZURE_SQL** configuration stanza from the credentials file.

```python
# initiates the AzureSQL class with the AZURE_SQL configs
azure_sql = AzureSQL(config_key="AZURE_SQL")
```

The above will pass all the configurations, including secrets like passwords, to the class. This avoids having to write secrets or configs in the code.

## Storing the file locally

Currently only local files are supported. Make sure to store the file in the correct path. 

* On Linux the path is `/home/viadot/.config/credentials.json`
* On Windows you need to create a `.config` folder with `credentials.json` inside the User folder `C:\Users\<user>`