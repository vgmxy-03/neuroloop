import asyncio
import atexit
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import bleak


_executor: Optional[ThreadPoolExecutor] = None


def _run(coro: Any):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    # Running inside an event loop; execute in a separate thread
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=1)
        # Ensure the executor is cleaned up on interpreter exit
        atexit.register(lambda: _executor and _executor.shutdown(wait=False))
    return _executor.submit(asyncio.run, coro).result()


class BleakBackend:
    """Minimal wrapper around bleak for device discovery only."""

    def scan(self, timeout: float = 10):
        devices = _run(bleak.BleakScanner.discover(timeout))
        return [{"name": d.name, "address": d.address} for d in devices]
