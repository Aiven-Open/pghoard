import time

from rohmu import BaseTransfer


class BlockingTransferFailingAfterTenSeconds(BaseTransfer):
    def __init__(self):
        super().__init__(prefix=self.__class__.__name__)

    def get_contents_to_string(self, key):
        """Returns a tuple (content-byte-string, metadata)"""
        time.sleep(10)
        raise ValueError("this should not happen a timeout should happen before (while testing)")


def alternate_get_transfer(storage_config) -> BaseTransfer:
    return BlockingTransferFailingAfterTenSeconds()
