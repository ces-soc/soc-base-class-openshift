# ces-soc-etl-base_class
Abstract Class for ETL Jobs for the CES-SOC

:information_source: You will need to inherit from this class and override the abstract `etl` function in order to use this class. The best example for using the base_class is the [etl-template](https://github.com/ces-soc/etl-template) repository.

## Features and Examples
The Base Class provides the following features:

* Automatic Healthcheck Reporting to Humio
    * Available in any campus via the `service-health` repo. You should access and create alerts against this repo via the `seceng` view.
* Easy writing to Humio
    * Pass your items as a list into the `write_humio` function
    * Optionally (but recommended): Define your path for easy searchability (available via `#path` or `_path` query in humio), as shown below:
    ```py
    self.write_humio(my_list_data, path="MyService")
    ```
    * Optionally, Override both Endpoint and Token, as shown below:
    ```py
    self.write_humio(my_list_data, path="MyService", endpoint="myendpoint.com", token="mytoken")
    ```
* Easy writing to S3
    * Using the `etl_destination`, and `intel_file` values in your config.json, it will take care of writing to s3 for you:
    ```py
    self.write_s3(my_json_data)
    ```
    * Or you can override one or both:
    ```py
    self.write_s3(my_json_data, etl_destination="my-cool-bucket", intel_file="intel.json")
    ```
* Get values from the parameter store (If you cannot use the configuration_manager):
    ```py
    my_value = self.get_value("/ces/myexternalservice/secret")
    ```
    * Or if you need permissions in addition to the default ETL role to get the secret:
    ```py
    access_key = self.config["myservice/externalkey"]
    secret_key = self.config["myservice/externalsecret"]
    my_value = self.get_value("/ces/myexternalservice/secret", access_key=access_key, secret_key=secret_key)
    ```
* Get values in a path from the parameter store (If you cannot use the configuration_manager):
    ```py
    my_value = self.get_parameters_of_path("/byu/myexternalservice")
    ```
    * Or if you need permissions in addition to the default ETL role to get the secret:
    ```py
    access_key = self.config["myservice/externalkey"]
    secret_key = self.config["myservice/externalsecret"]
    my_value = self.get_parameters_of_path("/byu/myexternalservice", access_key=access_key, secret_key=secret_key)
    ```
* Put values to parameter store (If you need to keep some state between tasks):
    * ETLs can only, by default, put values to the `etl-storage` path:
    ```py
    self.put_value("/byu/etl-storage/my_last_timestamp")
    ```
    * Or if you need permissions in addition to the default ETL role to put a value:
    ```py
    access_key = self.config["myservice/externalkey"]
    secret_key = self.config["myservice/externalsecret"]
    self.put_value("/byu/myexternalservice/secret", "MyParameterValue", access_key=access_key, secret_key=secret_key)
    ```