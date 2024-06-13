
### Prerequisites

We assume that you have [Rye](https://rye-up.com/) installed:

```console
curl -sSf https://rye-up.com/get | bash
```

### Installation

Clone the `2.0` branch, and set up and run the environment:

```console
git clone https://github.com/dyvenia/viadot.git -b 2.0 && \
  cd viadot && \
  rye sync
```

### Configuration

In order to start using sources, you must configure them with required credentials. Credentials can be specified either in the viadot config file (by default, `$HOME/.config/viadot/config.yaml`), or passed directly to each source's `credentials` parameter.

You can find specific information about each source's credentials in [the documentation](https://dyvenia.github.io/viadot/references/sql_sources/).