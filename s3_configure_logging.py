#! /usr/bin/env python
# s3_configure_logging.py
""" Configures logging for all S3 Buckets.

"""

import argparse, sys

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError

def set_logging(target, s3):
	""" Iterates through all the buckets under the account and sets up 
	logging to the target bucket.  Target skips itself.

	:param target: boto.s3.Bucket that is the location to log to.

	:param s3: boto.s3.S3Connection object

	"""
	for bucket in s3.get_all_buckets():
		if bucket.name != target.name:
			prefix = "%s/" % bucket.name
			bucket.enable_logging(target, prefix)

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description=__doc__)

	target_help = "Name of the bucket to log to."
	parser.add_argument('--target', '-t', dest="target", required=True,
		help=target_help)

	args = vars(parser.parse_args())

	s3 = S3Connection()
	try:
		target = s3.get_bucket(args['target'])
		set_logging(target, s3)
	except S3ResponseError as e:
		print("Error using target bucket %s: %s" % (args['target'], e))