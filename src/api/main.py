from fastapi import FastAPI, File, UploadFile
from minio import Minio
from minio.error import (
    ResponseError,
    BucketAlreadyOwnedByYou,
    BucketAlreadyExists)
from s3fs import S3FileSystem
import asyncio
import os
import shutil
from pathlib import Path, PurePosixPath, PurePath
from tempfile import NamedTemporaryFile
from typing import Callable

MINIO_ACCESS_KEY='AKIAIOSFODNN7EXAMPLE'
MINIO_SECRET_KEY='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
BUCKET = 'my-bucket'

app = FastAPI()


class LocalFileStorage(object):
    __s3: S3FileSystem = None

    def __init__(self: 'LocalFileStorage') -> None:
        loop = asyncio.get_event_loop()
        self.__s3 = S3FileSystem(anon=False, key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY, asynchronous=True, loop=loop, client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'})

    async def create_bucket_async(self: 'LocalFileStorage', bucket_name: str) -> None:
        async with await self.__s3._connect():
            await self.__s3._mkdir(path=bucket_name)

    async def create_file_async(self: 'LocalFileStorage', origin_file_name: str, file_path: Path, to_bucket: str) -> None:
        path = PurePosixPath()
        s3_file_path = path.joinpath(to_bucket, origin_file_name)
        async with await self.__s3._connect():
            await self.__s3._put_file(lpath=file_path, rpath=s3_file_path)

    async def delete_file_async(self: 'LocalFileStorage') -> None:
        async with await self.__s3._connect():
            files_in_bucket = await self.__s3._ls(BUCKET)
            for deleted_file in files_in_bucket:
                print(deleted_file['name'])
                await self.__s3._rm(paths=["s3://my-bucket/zvezdnoe_nebo_mlechnyj_put_zvezdy_blesk_kosmos_118653_5257x3474.jpg"])


localFileStorage = LocalFileStorage()

@app.get("/testminio/")
async def test_async():
    await localFileStorage.create_bucket_async(BUCKET)

@app.get("/")
async def ping_async():
    return {"Status": "Running"}

@app.post("/createfile/")
async def create_file_async(file: bytes = File(...)):
    return {"file_size": len(file)}

@app.post("/uploadfile/")
async def create_file_upload_async(file: UploadFile = File(...)):
    file_path_object = save_upload_file_tmp(file)

    await localFileStorage.create_file_async(file.filename, file_path_object, BUCKET)

    test_path = PurePath()
    test = test_path.joinpath("my-bucket", file.filename)
    return {"file_name": file.filename, "s3_file_path": test}

@app.delete("/deletefiles/")
async def delete_files_upload_async():
    await localFileStorage.delete_file_async()

    return {"deleted": "all"}


def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    try:
        with destination.open("wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)
    finally:
        upload_file.file.close()


def save_upload_file_tmp(upload_file: UploadFile) -> Path:
    try:
        suffix = Path(upload_file.filename).suffix
        with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            shutil.copyfileobj(upload_file.file, tmp)
            tmp_path = Path(tmp.name)
    finally:
        upload_file.file.close()
    return tmp_path


def handle_upload_file(
    upload_file: UploadFile, handler: Callable[[Path], None]
) -> None:
    tmp_path = save_upload_file_tmp(upload_file)
    try:
        handler(tmp_path)  # Do something with the saved temp file
    finally:
        tmp_path.unlink()  # Delete the temp file