# All files larger than this will use multipart uploading.
MULTIPART_CUTOFF = 5242880

# Size of the individual pieces in a multipart upload.
#
# Needs to be constant to make etag generation reproducible
# locally on files that were uploaded with multipart, as s3 uses
# the hash of the concatenated hashes of each part.
#
# Minimum: 5242880 (any smaller and s3 returns an error)
MULTIPART_PART_SIZE = 5242880

# Allowed attributes on s3tup.key.Key objects.
# Used to filter out invalid kwargs in the Key and KeyConfigurator
# constructors, and also acts as a guide for which attributes to set
# in KeyConfigurator.configure_key.
KEY_ATTRS = (
    'acl',
    'cache_control',
    'canned_acl',
    'content_disposition',
    'content_encoding',
    'content_type',
    'content_language',
    'encrypted',
    'expires',
    'metadata',
    'redirect_url',
    'reduced_redundancy',
)

# Allowed attributes on s3tup.bucket.Bucket objects.
# Used to filter out invalid kwargs in the Bucket constructor.
BUCKET_ATTRS = (
    'acl',
    'canned_acl',
    'cors',
    'lifecycle',
    'logging',
    'notification',
    'policy',
    'redirects',
    'region',
    'requester_pays',
    'tagging',
    'versioning',
    'website',
)
