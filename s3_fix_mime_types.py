#! /usr/bin/env python
# s3_fix_mime_types.py
"""
A one-off script to fix mime types on a handful of S3 objects.
This can be adapted later for more generalized use.
"""

from boto.s3.connection import S3Connection
from collections import defaultdict

va_bucket_name = "aptrust.preservation.storage"
or_bucket_name   = "aptrust.preservation.oregon"

files = (("uc.edu/cin.702780/data/1924/contents", "text/plain", "ff1c1ec1-b655-46a3-4cfb-baaa8fb5e9b6"),
("uc.edu/cin.702780/data/1906/b23_f23_p001.tif", "image/tiff", "e9ce75c7-1c43-41d0-74db-5511cc7114d9"),
("uc.edu/cin.702780/data/1828/contents", "text/plain", "60b83c37-74a7-4e96-698a-569bd8f47133"),
("uc.edu/cin.702780/data/1797/handle", "text/plain", "b7d176d8-b389-40ba-5987-ea4bcec253e3"),
("uc.edu/cin.702780/data/1778/b21_f63_p002.jpg", "image/jpeg", "f6d97571-55a3-4b4e-4bcb-f1ccebc3185a"),
("uc.edu/cin.702780/data/1762/b31_f04_n007.jp2.jpg", "image/jpeg", "660ab9d3-b06f-4732-4506-97e54091ad53"),
("uc.edu/cin.702780/data/1741/b28_f10_p003.jp2", "image/jp2", "6cb0708b-f712-46e2-4212-42a239e6bba1"),
("uc.edu/cin.702780/data/1741/b28_f10_p003.jp2.jpg", "image/jpeg", "78cd84cf-185b-4984-5171-8781fee0bae7"),
("uc.edu/cin.702780/data/1725/b21_f23_p004.tif", "image/tiff", "ad4428ef-46ae-4df5-728f-c611cf612862"),
("uc.edu/cin.702780/data/1692/b36_f44_n001.jpg.jpg", "image/jpeg", "34148582-b185-4ce1-50e1-0ff68262628f"),
("uc.edu/cin.702780/data/1692/b36_f44_n001.jpg", "image/jpeg", "e6cb3da7-b2d8-4088-5e9e-50a10408e4ac"),
("uc.edu/cin.702780/data/1663/b39_f19_n003.jpg", "image/jpeg", "1387b54a-4874-4f77-6db0-b70c8e1af7a7"),
("uc.edu/cin.702780/data/1560/b40_f03_n002.tif", "image/tiff", "6e20d99c-bbee-4f77-7be8-928918e190ac"),
("uc.edu/cin.702780/data/1533/b41_f03_n001.jp2.jpg", "image/jpeg", "4ef9190d-4486-4a5f-78c9-defaeda4659b"),
("uc.edu/cin.702780/data/152/b38_f45_n003.jpg", "image/jpeg", "460c6461-1ede-4b82-4b73-517e96f8fd05"),
("uc.edu/cin.702780/data/1512/dublin_core.xml", "application/xml", "594a7bd9-f8e0-4d48-5c83-eeb43234ae43"),
("uc.edu/cin.702780/data/1442/b43_f25_n002.jpg", "image/jpeg", "0cf6b2fe-b0d6-4c9a-4dec-f7066744dd71"),
("uc.edu/cin.702780/data/1430/b39_f14_n006.jpg", "image/jpeg", "62ab14b0-734c-4a59-62c5-f3de4294e114"),
("uc.edu/cin.702780/data/1425/b39_f14_n003.jpg", "image/jpeg", "6562cfbe-d5b3-4746-5731-e487417fcd0e"),
("uc.edu/cin.702780/data/1373/b45_f51_n002.tif", "image/tiff", "f73ac7f5-502f-4e04-40be-2aba831c48a5"),
("uc.edu/cin.702780/data/1326/b50_f34_n005.jpg.jpg", "image/jpeg", "71c6a078-038f-473b-40d6-c55bc2776967"),
("uc.edu/cin.702780/data/1326/b50_f34_n005.jp2", "image/jp2", "3c010be0-4ca8-417f-68a8-017c74c7687c"),
("uc.edu/cin.702780/data/1241/b42_f29_n004.tif", "image/tiff", "93bfe219-007a-42a6-5e3f-764a63fc85b5"),
("uc.edu/cin.702780/data/1238/dublin_core.xml", "application/xml", "12224ddc-94e4-42d9-780a-21608e896d04"),
("uc.edu/cin.702780/data/1238/b33_f36_n005.tif", "image/tiff", "cca56b34-850a-4089-77c4-6ebf19bc7488"),
("uc.edu/cin.702780/data/1231/contents", "text/plain", "e7bf46e0-6884-4af4-7dc5-1a514f38f7ef"),
("uc.edu/cin.702780/data/1114/b44_f39_n001.jp2", "image/jp2", "d3edeaeb-cb4e-4d73-7660-9f731633d0d5"),
("uc.edu/cin.702780/data/1045/handle", "text/plain", "b0913d40-0b8e-4c74-702f-cee3bbd071b1"))

def update_mime_type(bucket, uuid, mime_type):
    key = bucket.get_key(uuid)
    print("{0} => {1}".format(key.content_type, mime_type))
    #metadata = key.metadata
    #metadata["Content-Type"] = mime_type
    #header_data = {"Content-Type": mime_type}
    #bucket.copy_key(uuid, va_bucket_name, uuid, headers=header_data, metadata=metadata)

if __name__ == "__main__":
    s3 = S3Connection()
    va_bucket = s3.get_bucket(va_bucket_name)
    or_bucket = s3.get_bucket(or_bucket_name)
    for f in files:
        identifier = f[0]
        mime_type = f[1]
        uuid = f[2]
        update_mime_type(va_bucket, uuid, mime_type)
        update_mime_type(or_bucket, uuid, mime_type)
