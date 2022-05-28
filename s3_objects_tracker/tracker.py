import json
from contextlib import AsyncExitStack
from typing import Generic, List, Protocol, TypeVar

from aiobotocore import session

IDT = TypeVar("IDT", str, int)


class ObjectWithIDProtocol(Protocol[IDT]):
    id: IDT


T = TypeVar("T", bound=ObjectWithIDProtocol)


class S3ObjectsTracker(Generic[IDT]):
    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        filename: str = "data.json",
        max_published_objects: int = 50,
    ):
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.filename = filename
        self.max_published_objects = max_published_objects

        self._exit_stack = AsyncExitStack()
        self._s3_client: session.AioBaseClient
        self._published_ids: List[IDT]

    async def _fetch_from_s3(self) -> None:
        try:
            response = await self._s3_client.get_object(Bucket=self.bucket_name, Key=self.filename)
        except self._s3_client.exceptions.NoSuchKey:
            self._published_ids = []
            return

        async with response["Body"] as stream:
            self._published_ids = json.loads(await stream.read())

    async def _upload_to_s3(self) -> None:
        self._published_ids = self._published_ids[-self.max_published_objects :]
        json_str = json.dumps(self._published_ids) + "\n"
        await self._s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.filename,
            Body=json_str.encode("utf-8"),
            ContentType="application/json",
        )

    async def __aenter__(self) -> "S3ObjectsTracker":
        boto_session = session.get_session()
        self._s3_client = await self._exit_stack.enter_async_context(
            boto_session.create_client(
                service_name="s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        )
        await self._fetch_from_s3()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._upload_to_s3()
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    def determine_new(self, objects: List[T]) -> List[T]:
        new_objects = []
        for _object in objects:
            if _object.id not in self._published_ids:
                new_objects.append(_object)
        return new_objects

    async def publish(self, _object: T) -> None:
        self._published_ids.append(_object.id)
