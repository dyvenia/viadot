# Using config

In order to avoid storing and passing credentials through variables, source configuration is stored in the viadot config file (by default, in `~/.config/viadot/config.yaml`).

You can find each source's configuration in [the documentation](../references/sources/sql_sources.md).

Below is an example config file, with configurations for two sources:

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

In the above, `exchange_rates` and `sharepoint` are what we refer to as "config keys". For example, this is how to use the `exchange_rates` config key to pass credentials to the `ExchangeRates` source:

```python
# sources/exchange_rates.py

source = ExchangeRates(config_key="exchange_rates")
```

This will pass the `credentials` key, including the `api_key` secret, to the instance.

!!! info

    You can use any name for your config key, as long as it's unique. For example, we can have credentials for two different environments stored as `sharepoint_dev` and `sharepoint_prod` keys.
