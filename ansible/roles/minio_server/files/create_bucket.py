from minio import Minio
import sys

client = Minio(
    sys.argv[1],
    access_key=sys.argv[2],
    secret_key=sys.argv[3],
    secure=False
)

exists = client.bucket_exists(sys.argv[4])
if not exists:
    client.make_bucket(sys.argv[4])
print('created' if not exists else 'exists')