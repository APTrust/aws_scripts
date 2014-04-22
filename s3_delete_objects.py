#! /usr/bin/env python
# s3_delete_objects.py
""" Small script that will delete all objects in a named S3 bucket.  

--bucket, -b:  Name of the s3 bucket to delete objects from.

-f:  Optional flag to skip the confirmation message.
"""

import argparse, sys
from boto.s3.connection import S3Connection

def delete_all_objects(bucket):
	""" Iterates through all objects in the specified bucket
	and deletes it.

	:param bucket: string of bucket name.

	"""
	s3 = S3Connection()
	bucket = s3.get_bucket(bucket)
	for key in bucket.list():
		print("Deleting %s" % key.name)
		key.delete()

def confirm(question):
    "Confirms the deletion of objects in a bucket for safty purposes."
    valid = ["yes", "y", "ye",]

    sys.stdout.write(question + ' [y/N]:\n')
    choice = raw_input().lower()

    if choice in valid:
    	return True

    sys.stdout.write("Unable to confirm operation, canceling!!\n")
    return False

if __name__ == "__main__":
	help = """
	Deletes all objects in the S3 bucket provides during invocation.

	WARNING USE WITH CAUTION
	"""
	parser = argparse.ArgumentParser(description=help)

	bucket_help = "Name of bucket to delete objects from."
	parser.add_argument('--bucket', '-b', dest='bucket_name', required=True, 
		help=bucket_help)

	force_help = "Forces delete without confirmation."
	parser.add_argument('-f', dest="force", default=False, action='store_true', 
		help=force_help)

	args = vars(parser.parse_args())

	msg = ("WARNING: ABOUT TO DELETE CONTENTS OF S3 BUCKET %s ... confirm this operation?" % args['bucket_name'])
	proceed = confirm(msg)

	if args['force'] or proceed:
		delete_all_objects(args['bucket_name'])