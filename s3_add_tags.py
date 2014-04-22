#! /usr/bin/env python
# s3_add_tags.py
""" Small one time script to add restore and receiving tags to an S3 bucket.

"""

from boto.s3.connection import S3Connection
from boto.s3 import tagging


if __name__ == "__main__":
	s3 = S3Connection()
	buckets = s3.get_all_buckets()

	ts_restore = tagging.TagSet()
	ts_restore.add_tag("type", "restore")

	ts_receiving = tagging.TagSet()
	ts_receiving.add_tag("type", "receiving")

	restore = tagging.Tags()
	restore.add_tag_set(ts_restore)
	receiving = tagging.Tags()
	receiving.add_tag_set(ts_receiving)

	for b in buckets:
		parts = b.name.split('.')
		if parts[1] == 'restore': b.set_tags(restore)
		if parts[1] == 'receiving': b.set_tags(receiving)

	print "Done"