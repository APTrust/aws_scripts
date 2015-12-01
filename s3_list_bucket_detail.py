#! /usr/bin/env python
# s3_list_buckets.py
"""
Lists all keys in the specified bucket, along with each key's metadata.
Saves it all into a sqlite3 database.
"""

import sqlite3
import sys
from boto.s3.connection import S3Connection


def list_bucket(bucket_name):
    conn = sqlite3.connect('aptrust_s3.db')
    create_db_if_necessary(conn)
    s3 = S3Connection()
    bucket = s3.get_bucket(bucket_name)
    count = 0
    for key in bucket.list():
        full_key_with_metadata = bucket.get_key(key.name)
        pk = add_to_db(conn, full_key_with_metadata)
        print("{0:08d}  {1}".format(pk, key.name))
        count += 1
        if count > 10:
            return

def add_to_db(conn, key):
#    pk = 1
#    query = "select last_insert_rowid()"
#    c = conn.cursor()
#    c.execute(query)
#    row = c.fetchone()
#    if row and row[0]:
#        pk = row[0] + 1
    c = conn.cursor()
    statement = """insert into s3_keys
    (bucket, name, cache_control, content_type, etag,
    last_modified, storage_class, size)
    values (?,?,?,?,?,?,?,?)"""
    c.execute(statement, (key.bucket.name, key.name,
                          key.cache_control, key.content_type,
                          key.etag.replace('"', ''),
                          key.last_modified, key.storage_class,
                          key.size))
    conn.commit()
    pk = c.lastrowid
    for k,v in key.metadata.iteritems():
        statement = """insert into s3_meta (key_id, name, value)
        values (?,?,?)"""
        conn.execute(statement, (pk, k, v))
        conn.commit()
    c.close()
    return pk

def create_db_if_necessary(conn):
    query = """SELECT name FROM sqlite_master WHERE type='table'
    AND name='s3_keys'"""
    c = conn.cursor()
    c.execute(query)
    row = c.fetchone()
    if not row or len(row) < 1:
        print("Creating table s3_keys")
        statement = """create table s3_keys(
        id integer primary key autoincrement,
        bucket text, name text, cache_control text,
        content_type text,
        etag text, last_modified datetime,
        storage_class text, size int)"""
        conn.execute(statement)
        conn.commit()

        print("Creating table s3_meta")
        statement = "create table s3_meta(key_id, name, value)"
        conn.execute(statement)
        conn.commit()
    c.close()

if __name__ == "__main__":
    list_bucket('aptrust.preservation.storage')
    list_bucket('aptrust.preservation.oregon')
