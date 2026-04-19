import sys
from minio import Minio

client = Minio(
    sys.argv[1],
    access_key=sys.argv[2],
    secret_key=sys.argv[3],
    secure=False
)

client.fput_object(sys.argv[4], sys.argv[5], sys.argv[6])
print('uploaded')