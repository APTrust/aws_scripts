#
# s3_check_existence.py
#
# This script tells whether or not a file exists in 
# our DPN preservation bucket. This is for spot checking
# ProcessedItems with errors. 
#
from boto.s3.connection import S3Connection

# TODO: Read keys from text file.

keys = [
"df316e9f-ef22-4b70-b69c-e8c9236afb09.tar",
"f313f75a-696e-48bb-9204-3c9d2e5acc40.tar",
"8e29bd2f-25e3-4b73-8079-d3f1add402e5.tar"
]

s3 = S3Connection()
bucket = s3.get_bucket('aptrust.dpn.preservation')

for key in keys:
	objs = list(bucket.list(prefix=key))
	if len(objs) > 0 and objs[0].key == key:
		print("OK      -> {0}".format(key))
	else:
		print("MISSING -> {0}".format(key))

