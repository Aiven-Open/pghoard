from setuptools import setup, find_packages
from pghoard import __version__
import os

setup(
    name = "pghoard",
    version = os.getenv("VERSION") or __version__,
    zip_safe = False,
    packages = find_packages(exclude=["test"]),
    install_requires = [
        'psycopg2 >= 2.0.0',
        'requests >= 1.2.0',
        ],
    extras_require = {},
    dependency_links = [],
    package_data = {},
    entry_points = {
        'console_scripts': [
            "pghoard = pghoard.__main__",
            "pghoard_archive_command = pghoard.archive_command:main",
            "pghoard_archive_sync = pghoard.archive_sync:main",
            "pghoard_create_keys = pghoard.create_keys:main",
            "pghoard_restore = pghoard.restore:main",
        ],
    }
)
