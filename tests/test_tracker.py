import pytest

from s3_objects_tracker import S3ObjectsTracker


class Object:
    def __init__(self, _id: int):
        self.id = _id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Object):
            return NotImplemented
        return self.id == other.id

    def __repr__(self):
        return f"Object({self.id})"


@pytest.mark.asyncio
async def test_common_workflow(object_storage_mock, s3_credentials):
    """
    File exists in S3, but empty. Put 10 objects.
    Connect to S3. Determine new objects. They should be successfully published.
    """
    objects = [Object(i) for i in range(10)]
    async with S3ObjectsTracker(**s3_credentials) as tracker:
        assert tracker._published_ids == [], "Published IDs should be empty"
        new_objects = tracker.determine_new(objects)
        assert new_objects == objects, "New objects should be the same as objects"
        for _object in new_objects:
            await tracker.publish(_object)

    next_objects = [Object(i) for i in range(5, 15)]
    async with S3ObjectsTracker(**s3_credentials) as tracker:
        assert tracker._published_ids == [
            _object.id for _object in objects
        ], "First objects should be published"
        new_objects = tracker.determine_new(next_objects)
        assert new_objects == next_objects[5:], "Only last 5 objects should be new"
        for _object in new_objects:
            await tracker.publish(_object)

    async with S3ObjectsTracker(**s3_credentials) as tracker:
        assert tracker._published_ids == [_id for _id in range(15)], "All objects should be published"


@pytest.mark.asyncio
async def test_limit(object_storage_mock, s3_credentials):
    """
    File exists in S3, but empty. We have a limit of 5 objects. Put 6 objects.
    """
    objects = [Object(i) for i in range(5)]
    async with S3ObjectsTracker(**s3_credentials, max_published_objects=5) as tracker:
        assert tracker._published_ids == [], "Published IDs should be empty"
        new_objects = tracker.determine_new(objects)
        assert new_objects == objects, "New objects should be the same as objects"
        for _object in objects:
            await tracker.publish(_object)

    next_objects = [Object(i) for i in range(6)]
    async with S3ObjectsTracker(**s3_credentials, max_published_objects=5) as tracker:
        assert tracker._published_ids == [0, 1, 2, 3, 4], "Objects should be published"
        new_objects = tracker.determine_new(next_objects)
        assert new_objects == [Object(5)], "Only last _object should be new"
        await tracker.publish(new_objects[0])

    async with S3ObjectsTracker(**s3_credentials, max_published_objects=5) as tracker:
        assert tracker._published_ids == [1, 2, 3, 4, 5], "first object popped, last object pushed"


@pytest.mark.asyncio
async def test_empty_object_storage(empty_object_storage_mock, s3_credentials):
    async with S3ObjectsTracker(**s3_credentials) as tracker:
        assert tracker._published_ids == []


@pytest.mark.asyncio
async def test_exception_during_ctx(object_storage_mock, s3_credentials):
    try:
        async with S3ObjectsTracker(**s3_credentials) as tracker:
            await tracker.publish(Object(1))
            raise Exception("Test exception")
    except Exception:
        pass

    async with S3ObjectsTracker(**s3_credentials) as tracker:
        assert tracker._published_ids == [1], "First object should be published"
