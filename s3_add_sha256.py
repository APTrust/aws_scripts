#! /usr/bin/env python
# s3_fix_mime_types.py
"""
A one-off script add sha-256 digest metadata to a number of
S3 and Glacier files.
"""

from boto.s3.connection import S3Connection
from collections import defaultdict

va_bucket_name = "aptrust.preservation.storage"
or_bucket_name   = "aptrust.preservation.oregon"

def set_sha256(bucket, uuid, sha256):
    key = bucket.get_key(uuid)
    metadata = key.metadata
    metadata["sha256"] = sha256
    header_data = {"Content-Type": key.content_type }
    print("{0}   {1}  {2}".format(uuid, metadata, header_data))
    bucket.copy_key(uuid, va_bucket_name, uuid, headers=header_data, metadata=metadata)

if __name__ == "__main__":
    s3 = S3Connection()
    va_bucket = s3.get_bucket(va_bucket_name)
    or_bucket = s3.get_bucket(or_bucket_name)
    count = 0
    with open("files_sha256.txt") as f:
        for line in f:
            data = line.split(",")
            uuid = data[0].strip()
            sha256 = data[1].strip()
            set_sha256(va_bucket, uuid, sha256)
            set_sha256(or_bucket, uuid, sha256)
            count += 1
            if count > 10:
                break
