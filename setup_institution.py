#! /usr/bin/env python
# setup_institution.py
""" Sets up a new aptrust institution with appropriate AWS services.

Overall it:
	*  Creates appropriate S3 Receiving and Restoration buckets.
	*  Creates appropraite IAM Group.
	*  Creates appropriate IAM User and adds them to the Group.
"""

import argparse

from boto.s3.connection import S3Connection
from boto.exception import S3CreateError
from boto.iam.connection import IAMConnection

def _receiving_policy_name(domain):
	return "%s-receiving_bucket" % domain

def _receiving_policy_json(domain):
	json = '''{
		  "Version": "2012-10-17",
		  "Statement": [
		    {
		      "Effect": "Allow",
		      "Action": [
		        "s3:ListBucket"
		      ],
		      "Resource": [
		        "arn:aws:s3:::%s"
		      ]
		    },
		    {
		      "Effect": "Allow",
		      "Action": [
		        "s3:DeleteObject",
		        "s3:PutObject"
		      ],
		      "Resource": [
		        "arn:aws:s3:::%s/*"
		      ]
		    }
		  ]
		}''' % (_receiving_bucket_name(domain), _receiving_bucket_name(domain))
	return json

def _restore_policy_name(domain):
	return "%s-restore_bucket" % domain

def _restore_policy_json(domain):
	json = '''{
		  "Version": "2012-10-17",
		  "Statement": [
		    {
		      "Effect": "Allow",
		      "Action": [
		        "s3:ListBucket"
		      ],
		      "Resource": [
		        "arn:aws:s3:::%s"
		      ]
		    },
		    {
		      "Effect": "Allow",
		      "Action": [
		        "s3:DeleteObject",
		        "s3:GetObject"
		      ],
		      "Resource": [
		        "arn:aws:s3:::%s/*"
		      ]
		    }
		  ]
		}''' % (_restore_bucket_name(domain), _restore_bucket_name(domain))
	return json

def _receiving_bucket_name(domain):
	return "aptrust.receiving.%s" % domain

def _restore_bucket_name(domain):
	return "aptrust.restore.%s" % domain

def _iam_group_name(domain):
	return "%s.users" % domain

def _iam_user_name(domain):
	return "s3.%s" % domain

def setup_iam_accounts(domain, iam):
	"Creates IAM Groups and Users"
	# This is rather verbose but I can only understand the API documents if
	# I step through it one thing at a time.

	# Create Group
	iam.create_group(_iam_group_name(domain))
	group = iam.get_group(_iam_group_name(domain))

	# Add policies to Group
	iam.put_group_policy(_iam_group_name(domain), _restore_policy_name(domain), _restore_policy_json(domain))
	iam.put_group_policy(_iam_group_name(domain), _receiving_policy_name(domain), _receiving_policy_json(domain))

	# Add Create User
	iam.create_user(_iam_user_name(domain))
	user = iam.get_user(_iam_user_name(domain))

	# Add User to Group
	iam.add_user_to_group(_iam_group_name(domain), _iam_user_name(domain))

def create_buckets(domain, s3):
	"Creates the actual S3 restore and receiving buckets."
	try:
		for name in [_receiving_bucket_name(domain), _restore_bucket_name(domain)]:
			bucket = s3.create_bucket(name)
			# bucket = s3.get_bucket(name)
			prefix = name
			bucket.enable_logging('aptrust.s3.logs', prefix)
	except S3CreateError as e:
		print "Error creating buckets! %s" % e

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description=__doc__)
	domain_help = "Insitutional root domain to use for user and bucket naming conventions."
	parser.add_argument('--domain', '-d', dest="domain", required=True,
		help=domain_help)

	args = vars(parser.parse_args())

	s3 = S3Connection()
	create_buckets(args['domain'], s3)

	iam = IAMConnection()
	setup_iam_accounts(args['domain'], iam)