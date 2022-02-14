## Assumptions

* API can drop from time to time, so need to control for that within limits. If fails more than 3 times, then gracefully exit and try again on next schedule
* Key is sensitive and as such needs to be stored securely
* Bash script needed to add key to KMS so that it isn't stored in Github
* Everything fails all the time
  * Retry the api
  * fail gracefully
  * store the raw JSON if possible
  * Least permissions

## TODO
* Save raw json to S3
* Create cloud watch event
* Look at security
  * Can I add to KMS for auth key in api
* Look at IAM
* Update lambda job to get bucket from environment variable
* Neaten up code
  * Add comments
  * Make PEP8


## Useful notes
* When creating layers from python packages (on PIP) use the following code, this will ensure all the required parts of the package are installed the correct location for packaging up.
* ```
* pip install -t ./layer/python/lib/python3.7/site-packages [package_url.whl] -- note that the version needs to align with linux
* ```

* If the package is compiled
* [AWS Doco for dealing with compiled packages](https://aws.amazon.com/premiumsupport/knowledge-center/lambda-python-package-compatible)