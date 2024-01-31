import asyncio


class SpawningProxy:
    __slots__ = ("_items", "abort_on_error")

    def __init__(self, items, abort_on_error=False):
        self._items = items
        self.abort_on_error = abort_on_error

    def __getattr__(self, name):
        async def spawning_method(*args, **kwargs):
            tasks = [getattr(item, name)(*args, **kwargs) for item in self._items]

            if self.abort_on_error:
                return await asyncio.gather(*tasks)

            return [await result for result in asyncio.as_completed(tasks)]

        return spawning_method


class SpawningSet(set):
    """
    新增一个带有`property`属性的`all`方法的集合`set`，用于并发调用集合中的每个项目
    """
    @property
    def all(self):
        return SpawningProxy(self)
