from unittest import mock

import pytest


class StreamingBodyMock:
    def __init__(self, data):
        self.data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def read(self):
        return self.data

    async def put(self, data):
        self.data = data


class S3ClientMock(mock.Mock):
    def __init__(self):
        super().__init__()
        self.body = StreamingBodyMock(b"[]")

    async def get_object(self, **kwargs):
        return {'Body': self.body}

    async def put_object(self, **kwargs):
        await self.body.put(kwargs['Body'])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class S3SessionMock(mock.Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__mocked_client = S3ClientMock()

    def create_client(self, *args, **kwargs):
        return self.__mocked_client

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class ExceptionFactory:
    def __init__(self):
        class NoSuchKey(Exception):
            pass

        self.exceptions = {'NoSuchKey': NoSuchKey}

    def __getattr__(self, name):
        return self.exceptions[name]


class EmptyS3ClientMock(S3ClientMock):
    def __init__(self):
        super().__init__()
        self.exceptions = ExceptionFactory()

    async def get_object(self, **kwargs):
        raise self.exceptions.NoSuchKey


class EmptyS3SessionMock(S3SessionMock):
    def create_client(self, *args, **kwargs):
        return EmptyS3ClientMock()


@pytest.fixture
def object_storage_mock():
    s3SessionMock = S3SessionMock()
    with mock.patch("s3_objects_tracker.tracker.session.get_session", return_value=s3SessionMock):
        yield


@pytest.fixture
def empty_object_storage_mock():
    with mock.patch("s3_objects_tracker.tracker.session.get_session", return_value=EmptyS3SessionMock()):
        yield


@pytest.fixture(scope="session")
def s3_credentials():
    return {
        "bucket": "test-bucket",
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "test-access-key-id",
        "aws_secret_access_key": "test-secret-access-key",
    }
