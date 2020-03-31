from typing import Iterable, TypeVar, Generic

T = TypeVar('T')


class AsyncBase(Generic[T]):
    async def close(self) -> None:
        ...
    async def fileno(self) -> int:
        ...
    async def flush(self) -> None:
        ...
    async def isatty(self) -> bool:
        ...
    async def read(self, length: int=-1) -> T:
        ...
    async def readable(self) -> bool:
        ...
    async def readall(self) -> T:
        ...
    async def readinto(self, b: bytearray) -> int:
        ...
    async def readline(self) -> T:
        ...
    async def readlines(self) -> Iterable[T]:
        ...
    async def seek(self, offset: int, whence: int=0) -> int:
        ...
    async def seekable(self) -> bool:
        ...
    async def tell(self) -> int:
        ...
    async def truncate(self) -> None:
        ...
    async def writable(self) -> bool:
        ...
    async def write(self, data: T) -> int:
        ...
    async def writelines(self, lines: Iterable[T]) -> None:
        ...
