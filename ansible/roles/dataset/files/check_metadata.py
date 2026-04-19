import sys
from minio import Minio
from minio.error import S3Error

client = Minio(
    sys.argv[1],
    access_key=sys.argv[2],
    secret_key=sys.argv[3],
    secure=False
)

try:
    client.stat_object(sys.argv[4], sys.argv[5])
    print('exists')
except S3Error:
    print('missing')