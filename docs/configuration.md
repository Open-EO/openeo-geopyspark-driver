
# Configuration

## Background

The openEO GeoPySpark backend grew organically from a single instance proof of concept
to a component that is used in different deployments.
As a result, the (need for) configuration options also grew organically,
and the current implementation uses a mix of configuration approaches of varying flexibility:

- Just **hardcoded values** directly in the code that needs it.
  Obviously, this is all but flexible and needs to be avoided.
- Getting a configuration parameter from **environment variables directly**
  in the code that needs it (e.g. with `os.environ.get()` in Python).
  This allows for some basic flexibility, but
  the configuration option is hidden deep in the code and cumbersome to maintain
  (duplication, management of fallback values, ...)
- The **`ConfirParams` class** (in `openeogeotrellis/configparams.py`) was the first approach
  to have a **centralized** place for configuration parameters
  and improve the maintenance and overview challenges.

`ConfirParams` is mostly still based on reading and parsing environment variables.
This is fine for basic things like the URL of some service or database,
but it doesn't scale very well:

- the environment variables tend to be sprinkled all over the place:
  shell scripts, Python startup scripts, Jenkinsfiles, Dockerfiles, etc.,
  possibly across different repositories.
  It's hard to figure out where to find, add or update a particular setting.
- environment variables only work well for string parameters,
  but some configuration requires more complex constructs (lists, mappings, nested constructs)
  and trying to properly encode these in a single string as env variable is not always trivial
  (e.g. it must survive possible string manipulation in shell scripts along the way).


## Config files

To address the limitations of doing configuration through environment variables,
(yet) another configuration system was introduced
(under [#285](https://github.com/Open-EO/openeo-geopyspark-driver/issues/285))
based on configuration files.
**Python** was chosen as configuration file format (instead of JSON, YAML, TOML, ...)
for maximum flexibility and easy integration with the openeo-geopyspark-driver and related code
(e.g. type hints and code navigation features).
Configuration parameters can be defined directly as Python values,
not only the usual suspects like lists, dicts, tuples, sets, etc.
but also custom objects or constructs.
It also allows to derive parameters dynamically,
for example based on environment variables or other contextual information.

Note that this file based configuration system is introduced gradually
and the original `ConfirParams` is largely kept untouched.


### Usage

#### Setup

- A config file with this system is expected to define a `config` variable at top-level,
  and it must be an instance of the class `openeogeotrellis.config.GpsBackendConfig`.
  A minimal config file looks like this:
  ```python
  from openeogeotrellis.config import GpsBackendConfig
  config = GpsBackendConfig()
  ```
- When starting the openEO GeoPyspark backend,
  the environment variable `OPENEO_BACKEND_CONFIG` must be set
  contain the path to this config file.

Also look at the [default config file](https://github.com/Open-EO/openeo-geopyspark-driver/blob/master/openeogeotrellis/config/default.py)
which is loaded when `OPENEO_BACKEND_CONFIG` is not set.


#### Setting values

The `GpsBackendConfig` class is defined through
the popular [`attrs`](https://www.attrs.org/en/stable/) package
to compactly define attributes with type hints and default values.

For example:

```python
from openeo_driver.users.oidc import OidcProvider

@attrs.define
class GpsBackendConfig:
    oidc_providers: List[OidcProvider] = attrs.Factory(list)
```

Config parameter `oidc_providers` must be a list of `OidcProvider` instances,
and will be an empty list by default.
A custom config file that sets this parameter might look like this:

```python
from openeo_driver.users.oidc import OidcProvider
from openeogeotrellis.config import GpsBackendConfig

config = GpsBackendConfig(
    oidc_providers=[
      OidcProvider(id="OIDC", issuer="https://oidc.test", title="test OIDC"),
    ],
)
```
