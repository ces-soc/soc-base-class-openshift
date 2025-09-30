# Configuration Manager

This module is used to support the development of the ETL and EDM projects.

## ConfigManager Usage

Contained is an in-depth guide to the features provided through the config.json file.

### Required and Reserved Configuration Keys

Certain keys within config.json perform functions within the configuration manager and are therefore reserved. Using these keys in alternate ways may result in unexpected behavior.

These keys are located in the `config` base key (or in other words, are always in the config.json file)

* logging_severity **OPTIONAL**
  * Type: <string\> Restricted to: `debug`, `info`, `warning`. Informs Configuration Manager which logging level to use in accordance with the [Python Logging Library](https://docs.python.org/3/library/logging.html)
  * Defaults to `info`
* scope **OPTIONAL**
  * Type: <list\> Names of parameter store services/products, all child parameters of this service/product are pulled from parameter store and are stored in the `parameters` base key (described below in [Parameters](#parameters))
  * If any of these keys do not exists in the parameter store, the config manager will exit(1)
  * For example, specifying a scope of `/${CAMPUS}/service` would pull all parameters with `/${CAMPUS}/service/*`
* extra_parameters **OPTIONAL**
  * Type: <list\> Paths of parameter store values which are resolved and stored in the `parameters` base key (described below in [Parameters](#parameters))
  * If any of these keys do not exists in the parameter store, the config manager will exit(1)
* etl_destination (ETLs only) **REQUIRED** *if write_intel() function is used*
  * Type: <string\> Indicates the bucket to which the ETL class will write
* intel_file (ETLs only) **REQUIRED** *if write_intel() function is used*
  * Type <string\> Indicates the name of the intel file (including its path in s3)

### Configuration Manager Order of Operations

1. Resolve variables (Indicated by `${var}` syntax)
1. Resolve AWS Parameter Store values

### Parameters

The config manager will use the boto3 credentials (these are stored in the standard boto3 environment variables names `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` as described [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)) to attempt to get values for the parameters from the AWS parameter store.

The location for the parameter name is automatically remove for easier access in the calling code.

config (Before resolving required_parameters):

```json
{
    "logging_severity": "${LOGGING_SEVERITY}",
    "intel_file": "/intel/example.intel",
    "etl_destination": "ces-soc-etl-${CAMPUS}-${STAGE}",
    "scope": [
        "/${CAMPUS}/my_cool_service"
    ],
    "extra_parameters": [
        "/${CAMPUS}/my_other_service/password",
        "/${CAMPUS}/cool_product/username",
        "/${CAMPUS}/other_stuff/port"
    ]
}
```

config (After resolving required_parameters):

```json
{
    "logging_severity": "debug",
    "intel_file": "/intel/example.intel",
    "etl_destination": "ces-soc-etl-byu-dev",
    "scope": [
        "/byu/my_cool_service"
    ],
    "extra_parameters": [
        "/byu/my_other_service/password",
        "/byu/cool_product/username",
        "/byu/other_stuff/port"
    ],
    "parameters": {
        "my_cool_service/param1": "Brought in by scope",
        "my_cool_service/param2": "Also brought in by the scope entry",
        "my_cool_service/paramN": "Any N number of parameters can be imported using scope",
        "my_other_service/password": "Pa$$w0rd",
        "cool_product/username": "Gr00t",
        "other_stuff/port": 8089
    }
}
```
Note:
In this example, scope and extra_parameters perform the same type of function (accessing data from the parameter store); however, usage of scope is preferable when multiple parameters from the service/product will be accessed for performance reasons.

### Variables

Variables can be interpolated within the configuration files using HCL-like syntax, as seen below:

config (Before Interpolation)

```json
{
    "logging_severity": "${LOGGING_SEVERITY}",
    "intel_file": "/intel/example.intel",
    "etl_destination": "ces-soc-etl-${CAMPUS}-${STAGE}",
    "scope": [
        "global-service-account"
    ],
    "extra_parameters": [
        "/${CAMPUS}/global-service-account/password",
        "/${CAMPUS}/global-service-account/username"
    ]
}
```

config (After Interpolation and stored within self.config)

```json
{
    "logging_severity": "debug",
    "intel_file": "/intel/example.intel",
    "etl_destination": "ces-soc-etl-byup-dev",
    "scope": [
        "global-service-account"
    ],
    "extra_parameters": [
        "/byu/global-service-account/password",
        "/byu/global-service-account/username"
    ]
}
```

If the configuration manager cannot find an environment variable that matches (case-sensitive) the variable name entered in the config file, it will fail. For example, in example above the Dockerfile would need to have had `STAGE`, `CAMPUS`, and `LOGGING_SEVERITY` available as environment variables.

Most configurations can be access like the examples above:

```python
intel_file = self.config['intel_file']
```

### Logging

The config manager will try to setup the root logger. This can conflict when either the client code or a 3rd party module tries to configure the root logger as well. If there's a specific need to configure a different type of logger, use the `logging.getLogger(name_here)` method.

If the config manager detects the current session as a tty, then it will produce logs in color. Otherwise, all messages will be logged in json format with no color. This functionality can be overridden with the `force_json_logging` parameter when initializing the config manager.