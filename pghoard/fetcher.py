import multiprocessing
import os
import queue
import signal
import threading
import time

from pghoard.common import get_object_storage_config
from pghoard.config import key_lookup_for_site
from pghoard.rohmu import get_transfer
from pghoard.rohmu.rohmufile import create_sink_pipeline


class FileFetchManager:
    """Manages (potentially) multiprocessing related assets for fetching file contents from
    object storage. If a multiprocess.Manager instance is provided, the fetch is performed
    in a subprocess to avoid GIL related performance constraints, otherwise file is fetched
    in current process."""
    def __init__(self, app_config, mp_manager, transfer_provider):
        self.config = app_config
        self.last_activity = time.monotonic()
        self.lock = threading.RLock()
        self.max_idle_age = 10 * 60
        self.mp_manager = mp_manager
        self.process = None
        self.result_queue = None
        self.task_queue = None
        self.transfer_provider = transfer_provider

    def check_state(self):
        if self.process and time.monotonic() - self.last_activity > self.max_idle_age:
            self.stop()

    def fetch_file(self, site, key, target_path):
        self.last_activity = time.monotonic()
        self._start_process()
        if self.mp_manager:
            self.task_queue.put((site, key, target_path))
            result = self.result_queue.get()
            if result is None:
                # Should only happen if the process is terminated while we're waiting for
                # a result, which is pretty much the same as timeout
                raise queue.Empty
            elif isinstance(result[1], Exception):
                raise result[1]
            return result[1], result[2]
        else:
            transfer = self.transfer_provider(site)
            return FileFetcher(self.config, transfer).fetch(site, key, target_path)

    def stop(self):
        with self.lock:
            if not self.process:
                return
            self.task_queue.put(None)
            self.result_queue.put(None)
            process = self.process
            self.process = None
            self.task_queue = None
            self.result_queue = None
        process.join(timeout=0.1)
        if process.exitcode is None:
            os.kill(process.pid, signal.SIGKILL)
            process.join()

    def _start_process(self):
        with self.lock:
            if not self.mp_manager or self.process:
                return
            self.result_queue = self.mp_manager.Queue()
            self.task_queue = self.mp_manager.Queue()
            self.process = multiprocessing.Process(
                target=_remote_file_fetch_loop, args=(self.config, self.task_queue, self.result_queue)
            )
            self.process.start()


class FileFetcher:
    """Fetches a file from object storage and strips possible encryption and/or compression away."""
    def __init__(self, app_config, transfer):
        self.config = app_config
        self.transfer = transfer

    def fetch(self, site, key, target_path):
        try:
            lookup = key_lookup_for_site(self.config, site)
            data, metadata = self.transfer.get_contents_to_string(key)
            if isinstance(data, str):
                data = data.encode("latin1")
            file_size = len(data)
            with open(target_path, "wb") as target_file:
                output = create_sink_pipeline(
                    output=target_file, file_size=file_size, metadata=metadata, key_lookup=lookup, throttle_time=0
                )
                output.write(data)
            return file_size, metadata
        except Exception:
            if os.path.isfile(target_path):
                os.unlink(target_path)
            raise


def _remote_file_fetch_loop(app_config, task_queue, result_queue):
    transfers = {}
    while True:
        task = task_queue.get()
        if not task:
            return
        try:
            site, key, target_path = task
            transfer = transfers.get(site)
            if not transfer:
                transfer = get_transfer(get_object_storage_config(app_config, site))
                transfers[site] = transfer
            file_size, metadata = FileFetcher(app_config, transfer).fetch(site, key, target_path)
            result_queue.put((task, file_size, metadata))
        except Exception as e:  # pylint: disable=broad-except
            result_queue.put((task, e))
