#! /usr/bin/env python
# s3_list_buckets.py
""" Queries for all buckets accessible by this S3 account and outputs
the bucket name and total size of storage in that bucket.

"""

import math

from boto.s3.connection import S3Connection
from collections import defaultdict

def convertSize(num_bytes, unit='gb'):
	"Converts bytes to the unit scale provided."
	unit = unit.lower()
	if unit == 'b' or num_bytes < 1024: return "%d B" % num_bytes
	units = {
		'kb': 1, 'mb': 2, 'gb': 3, 'tb': 4, 'pb': 5, 'eb': 6,
	}
	f_bytes = float(num_bytes)
	for i in range(units[unit]):
		f_bytes = f_bytes / 1024

	return "%f %s" % (f_bytes, unit.upper())

if __name__ == "__main__":
	b = defaultdict(lambda: defaultdict(int))
	storage = {
		'count': 0,
		'total': 0,
		'largest': 0,
	}
	s3 = S3Connection()

	for bucket in s3.get_all_buckets():
		for key in bucket.list():
			b[bucket.name]['size'] += key.size
			b[bucket.name]['count'] += 1
			storage['count'] += 1
			storage['total'] += key.size
			if key.size > b[bucket.name]['largest']:
				b[bucket.name]['largest'] = key.size
			if key.size > storage['largest']:
				storage['largest'] = key.size

        format_str = "{0: <40} {1: >12} {2: >12} {3: >12}"
        print(format_str.format("Bucket", "Files", "Size", "Largest"))
	print("-" * 79)
	#for bucket, stats in b.iteritems():
        for bucket_name in sorted(b):
                if bucket_name == 'B': # where does this come from?
                        continue
                stats = b[bucket_name]
		print(format_str.format(bucket_name,
                                        stats['count'],
                                        convertSize(stats['size']),
                                        convertSize(stats['largest'])))

	print("-" * 79)
	print(format_str.format("Total:",
                                storage['count'],
                                convertSize(storage['total']),
                                convertSize(storage['largest'])))
