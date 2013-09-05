# s3tup

Python package that offers declarative configuration management for amazon s3.

## Installation

Install via pip:

    $ pip install s3tup

Install from source:

    $ git clone git://github.com/HeyImAlex/s3tup.git
    $ cd s3tup
    $ python setup.py

## Usage

S3tup provides both a command line client and a python api. Simply write out a config, set your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars and run:

    $ s3tup /path/to/your/config.yml

positional arguments:

* **config** - relative or absolute path to the config file

optional arguments:
- **-h, --help** - show this help message and exit
- **--access_key_id** &lt;access_key_id&gt; - your aws access key id. optional if 'AWS_ACCESS_KEY_ID' env var is set.
- **--secret_access_key** &lt;secret_access_key&gt; - your aws secret access key. optional if 'AWS_SECRET_ACCESS_KEY' env var is set.
- **--rsync_only** - only sync rsynced keys
- **-v, --verbose** - run at info log level
- **--debug** - run at debug log level

## Config File

The s3tup configuration file is plain yaml. The base is a list of bucket configurations which are defined below. An example configuration is available [here](https://github.com/HeyImAlex/s3tup/blob/master/example.yml) to help you and I'll try and keep it as up to date as possible. For more in depth details on what exactly all of these fields do you'll need to consult the [online documentation for s3](http://docs.aws.amazon.com/AmazonS3/latest/API/APIRest.html).

**Note**: Setting an option to `None` and not setting it at all are not the same thing. For many fields `None` will assert that the configuration option is not set at all.

#### Bucket Configuration

The bucket configuration is a dict that contains, predictably, the configuration options for the bucket named by the required field `bucket`. All other fields are optional.

field | default | description
:---- | :------ | :----------
bucket | required | The target bucket name.
region | '' | The region that the bucket is in.  Valid values: EU, eu-west-1, us-west-1, us-west-2, ap-southeast-1, ap-southeast-2, ap-northeast-1, sa-east-1, empty string (for the US Classic Region). Note that a bucket's region cannot change; s3tup will raise an exception if the bucket already exists and the regions don't match.
canned_acl | | The [canned acl](http://docs.aws.amazon.com/AmazonS3/latest/dev/ACLOverview.html#CannedACL) of the bucket. Valid values: private, public-read, public-read-write, authenticated-read, bucket-owner-read, bucket-owner-full-control.
website | | The website configuration of the bucket. Valid values: Either a string xml website configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTwebsite.html) page) or `None` which will delete the website configuration for this bucket all together.
cors | | The cors configuration of the bucket. Valid values: Either a string xml cors configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTcors.html) page) or `None` which will delete the cors configuration for this bucket all together.
lifecycle | | The lifecycle configuration of the bucket. Valid values: Either a string xml lifecycle configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTlifecycle.html) page) or `None` which will delete the lifecycle configuration for this bucket all together.
logging | | The logging configuration of the bucket. Valid values: Either a string xml logging configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTlogging.html) page) or `None` which will delete the logging configuration for this bucket all together.
notification | | The notification configuration of the bucket. Valid values: Either a string xml notification configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTnotification.html) page) or `None` which will delete the notification configuration for this bucket all together.
policy | | The policy set on this bucket. Valid values: Either a string json policy (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/dev/AccessPolicyLanguage_HowToWritePolicies.html) page) or `None` which will delete the policy from this bucket all together.
tagging | | The tagging configuration of the bucket. Valid values: Either a string xml tagging configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTtagging.html) page) or `None` which will delete all tags from this bucket.
versioning | | Boolean value that says wether to enable or disable versioning.
key_config | | Takes a list of key configuration dicts and applies them to all of the applicable keys in the bucket. See section Key Configuration for details.
rsync | | Takes an rsync configuration dict and "rsyncs" a folder with the bucket. See section Rsync Configuration for details.
redirects | [ ] | Takes a list of [key, redirect location] pairs and will create a zero byte object at `key` that redirects to whatever redirect location you specify.

#### Key Configuration

The key configuration field allows you to define key configurations that apply to all keys matched by your matcher fields. These configurations are applied in the order that they appear, and conflicting fields will be overwritten by whichever configuration was applied last. The bucket configuration takes a list of key configurations, so you can have as many as you like. Keep in mind that many of these options are not idempotent; if you already have configuration set on an s3 key, s3tup will overwrite it when it syncs.

field | default | description
:---- | :------ | :----------
matcher fields | | See section Matcher Fields below.
reduced_redundancy | False | Boolean option to use reduced redundancy storage.
encrypt | False | Boolean option to use server side encryption.
canned_acl | | The [canned acl](http://docs.aws.amazon.com/AmazonS3/latest/dev/ACLOverview.html#CannedACL) for the key.
acl |  | String xml acl policy for this key.
cache_control | None | String value of the cache-control header.
content_disposition | None | String value of the content-disposition header.
content_encoding | None | String value of the content-encoding header. S3tup will not guess content encoding.
content_language | None | String value of the content-language header.
content_type | None | String value of the content-type header. If not explicitly set, s3tup will make a best guess based on the extension.
expires | None | String value of the expires header.
metadata | { } | Dict of metadata headers to set on the key.

#### Rsync Configuration

The rsync field allows you to "rsync" a local folder with an s3 bucket. All keys that are uploaded are configured by any present key configurations. Remember that the rsync configuration definition contains the matcher fields and any keys not matched will not be rsynced. This is helpfull for ignoring certain files or folders during rsync (and basically emulates the inclue/exclude/rinclude/rexclude options of s3cmd's sync). The matching process is run on the destination key name, not the local pathname.

field | default | description
:---- | :------ | :----------
matcher fields | | See section Matcher Fields below.
src | required | Relative or absolute path to folder to rsync. Trailing slash is not important.
delete | False | Option to delete keys present in the bucket that are not present locally.

#### Matcher Fields

Both the key and rsync configuration definitions contain these optional fields to constrain which keys they act upon. These are intended to function as intuitively as possible, but in the name of explicitness:

If none of these fields are present, all keys are matched. If neither `patterns` nor `regexes` are present, all keys except those matched by `ignore_patterns` and `ignore_regexes` are matched. If either `patterns` or `regexes` are present, only keys that `patterns` or `regexes` match and are not matched by either `ignore_patterns` or `ignore_regexes` are matched. Whew.

field | default | description
:---- | :------ | :----------
patterns | None | List of unix style patterns to include
ignore_patterns | None | List of unix style patterns exclude
regexes | None | List of regex patterns to include
ignore_regexes | None | List of regex patterns exclude
