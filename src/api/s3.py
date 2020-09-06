from s3fs import S3FileSystem
import typing

class LocalFileStorage(object):
    __s3: S3FileSystem = None

    def __init__(self, s3fs: S3FileSystem = None) -> None:
        self.__s3 = s3fs or S3FileSystem(anon=False, client_kwargs={'endpoint_url': '127.0.0.1:9000'})

    async def create_bucket_async(self: 'LocalFileStorage', bucket_name: str) -> None:
        await self.__s3._mkdir(path=bucket_name)

    async def save_file_async(self: 'LocalFileStorage', file_path: str, to_bucket: str) -> None:
        await self.__s3._put(file_path, to_bucket + "/" + file_path)

