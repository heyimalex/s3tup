# s3tup

Python package that offers declarative configuration management and deployment for amazon s3.

## Why?

Because writing custom scripts for configuring and deploying to s3 through boto was a major pain. Though tools like s3sync exist, they lack robust options for configuration and you often still need some customization or outside scripting to get them to do exactly what you want.

## Installation

Install via pip:

    $ pip install s3tup

Install from source:

    $ git clone git://github.com/HeyImAlex/s3tup.git
    $ cd s3tup
    $ python setup.py

## Usage

S3tup can be used as a command line tool or a python library. Just write out a config file

    ---
    - bucket: example-bucket
      rsync:
        src: /path/to/folder

Set your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars and then run:

    $ s3tup /path/to/your/config.yml

Easy as that! The configuration file can be as simple or robust as you need, and there are a few examples in the repo to help you out.

Alternatively you can use s3tup as a library within python.

```python
from s3tup.connection import Connection
from s3tup.bucket import Bucket

conn = Connection()
b = Bucket(conn, 'test-bucket')
b.canned_acl = 'public-read'
b.sync()
```

Documentation here is lacking at the moment, but I'm working on it (and the source is a short read).

## Config File

The s3tup configuration file is plain yaml. The base is a list of bucket configurations which are defined below. An example configuration is available [here](https://github.com/HeyImAlex/s3tup/blob/master/example.yml) to help you and I'll try and keep it as up to date as possible. Because s3tup is just a thin wrapper over the s3 REST api, the best way to understand what all of these options actually do is to consult the [online documentation for s3](http://docs.aws.amazon.com/AmazonS3/latest/API/APIRest.html).

**Note**: Setting an option to `None` and not setting it at all are not the same thing. For many fields `None` will assert that the configuration option is not set at all.

#### Bucket Configuration

The bucket configuration is a dict that contains, predictably, the configuration options for the bucket named by the required field `bucket`. All other fields are optional.

field | default | description
:---- | :------ | :----------
bucket | required | The target bucket name.
region | '' | The region that the bucket is in.  Valid values: EU, eu-west-1, us-west-1, us-west-2, ap-southeast-1, ap-southeast-2, ap-northeast-1, sa-east-1, empty string (for the US Classic Region). Note that a bucket's region cannot change; s3tup will raise an exception if the bucket already exists and the regions don't match.
canned_acl | | The [canned acl](http://docs.aws.amazon.com/AmazonS3/latest/dev/ACLOverview.html#CannedACL) of the bucket. Valid values: private, public-read, public-read-write, authenticated-read, bucket-owner-read, bucket-owner-full-control.
website | | The website configuration of the bucket. Valid values: Either a string xml website configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTwebsite.html) page) or `None` which will delete the website configuration for this bucket all together.
acl | | The acl set on this bucket. Valid values: Either a string xml acl (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTcors.html) page) or `None`, which will set the defualt acl on the bucket.
cors | | The cors configuration of the bucket. Valid values: Either a string xml cors configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTcors.html) page) or `None` which will delete the cors configuration for this bucket all together.
lifecycle | | The lifecycle configuration of the bucket. Valid values: Either a string xml lifecycle configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTlifecycle.html) page) or `None` which will delete the lifecycle configuration for this bucket all together.
logging | | The logging configuration of the bucket. Valid values: Either a string xml logging configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTlogging.html) page) or `None` which will delete the logging configuration for this bucket all together.
notification | | The notification configuration of the bucket. Valid values: Either a string xml notification configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTnotification.html) page) or `None` which will delete the notification configuration for this bucket all together.
policy | | The policy set on this bucket. Valid values: Either a string json policy (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/dev/AccessPolicyLanguage_HowToWritePolicies.html) page) or `None` which will delete the policy from this bucket all together.
tagging | | The tagging configuration of the bucket. Valid values: Either a string xml tagging configuration (detailed on [this](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTtagging.html) page) or `None` which will delete all tags from this bucket.
versioning | | Boolean value that says wether to enable or suspend versioning. Note: Once versioning is enabled on a bucket it cannot be disabled, only suspended! Any bucket that has ever had versioning enabled cannot have a lifecycle configuration set!
key_config | | Takes a list of key configuration dicts and applies them to all of the applicable keys in the bucket. See section Key Configuration for details.
rsync | | Takes either an rsync configuration dict or a list of them and "rsyncs" a folder with the bucket. See section Rsync Configuration for details.
redirects | [ ] | Takes a list of [key, redirect location] pairs and will create a zero byte object at `key` that redirects to whatever redirect location you specify.

#### Key Configuration

The key configuration field allows you to define key configurations that apply to all keys matched by your matcher fields. These configurations are applied in the order that they appear, and conflicting fields will be overwritten by whichever configuration was applied last. The bucket configuration takes a list of key configurations, so you can have as many as you like. Keep in mind that many of these options are not idempotent; if you already have configuration set on an s3 key, s3tup will overwrite it when it syncs.

field | default | description
:---- | :------ | :----------
matcher fields | | See section Matcher Fields below.
reduced_redundancy | False | Boolean option to use reduced redundancy storage.
encrypted | False | Boolean option to use server side encryption.
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

The rsync field allows you to "rsync" a local folder with an s3 bucket. All keys that are uploaded are configured by any present key configurations. Remember that the rsync configuration definition contains the matcher fields and any keys not matched will not be rsynced. This is helpfull for ignoring certain files or folders during rsync (and basically emulates the inclue/exclude/rinclude/rexclude options of s3cmd's sync). The matching process is run on the local pathname relative to src.

field | default | description
:---- | :------ | :----------
matcher fields | | See section Matcher Fields below.
src | required | Relative or absolute path to folder to rsync. Trailing slash is not important.
dest | '' | Optional, allows you to rsync with a specific folder on s3.
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

## Cli

positional arguments:

* **config** - relative or absolute path to the config file

optional arguments:
- **-h, --help** - show this help message and exit
- **--access_key_id** &lt;access_key_id&gt; - your aws access key id
- **--secret_access_key** &lt;secret_access_key&gt; - your aws secret access key
- **--rsync_only** - only sync rsynced keys
- **-v, --verbose** - increase output verbosity
- **-q, --quiet** - silence all output

## TODO

This project is in early development and still has plenty of work before being production ready...

* Add tests
* Improve docs
* Can't currently handle file uploads larger than 2gb
* Requester pays not implemented
* Mfa delete not implemented