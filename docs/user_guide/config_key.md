

Credentials and other settings for various **sources** are stored in a file named `config.yaml`. A credential file needs to be written in `YAML` format. A typical credentials file looks like this:

```yaml
sources:

  - exchange_rates:
      class: ExchangeRates
      credentials:
        api_key: "api123api123api123"

  - sharepoint:
      class: Sharepoint
      credentials:
        site: "site.sharepoint.com"
        username: "user@email.com"
        password: "password"
```

In the above, `exchange_rates` and `sharepoint` are what we refer to as "config keys". These config settings are fed to the `ExchangeRates()` or `Sharepoint()` sources.

For example, this is how to use the `ExchangeRates()` configuration stanza from the config file.

```python
# initiates the ExchangeRates() class with the exchange_rates configs
rates = ExchangeRates(config_key="exchange_rates")
```

The above will pass all the configurations, including secrets like passwords, to the class. This avoids having to write secrets or configs in the code.

### Storing the file locally

Currently, only local files are supported. Make sure to store the file at the correct path. 

* On Linux the path is `~/.config/viadot/config.yaml`