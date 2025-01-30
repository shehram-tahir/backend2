from typing import Optional
import asyncio
from contextlib import asynccontextmanager
import aiofiles


import json
import os


class FileLock:
    def __init__(self):
        self.locks = {}

    @asynccontextmanager
    async def acquire(self, filename):
        if filename not in self.locks:
            self.locks[filename] = asyncio.Lock()
        async with self.locks[filename]:
            yield


file_lock_manager = FileLock()


async def use_json(
    file_path: str, mode: str, json_content: dict = None
) -> Optional[dict]:
    async with file_lock_manager.acquire(file_path):
        if mode == "w":
            try:
                async with aiofiles.open(file_path, mode="w") as file:
                    await file.write(json.dumps(json_content, indent=2))
            except IOError as e:
                raise Exception(f"Error reading data file: {str(e)}")

        elif mode == "r":
            try:
                if os.path.exists(file_path):
                    async with aiofiles.open(file_path, mode="r") as file:
                        content = await file.read()
                        return json.loads(content)
                return None
            except json.JSONDecodeError as e:
                raise Exception(f"Error parsing data file: {str(e)}")
            except IOError as e:
                raise Exception(f"Error reading data file: {str(e)}")
        else:
            raise ValueError("Invalid mode. Use 'r' for read or 'w' for write.")
