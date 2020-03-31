import asyncio
import concurrent
from os import PathLike
from typing import IO, Union, Optional, Any, Callable

from aiofiles.base import AsyncBase


async def open(file: Union[str, bytes, int, PathLike[Any]],
         mode: str = ...,
         buffering: int = ...,
         encoding: Optional[str] = ...,
         errors: Optional[str] = ...,
         newline: Optional[str] = ...,
         closefd: bool = ...,
         opener: Optional[Callable[[str, int], int]] = ...,
         *,
         loop: Optional[asyncio.AbstractEventLoop] = ...,
         executor: Optional[concurrent.futures.Executor] = ...) -> AsyncBase[Any]: ...
