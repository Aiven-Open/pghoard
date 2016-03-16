from setuptools import setup, find_packages
from pghoard import __version__
import os

setup(
    name="pghoard",
    version=os.getenv("VERSION") or __version__,
    zip_safe=False,
    packages=find_packages(exclude=["test"]),
    install_requires=[
        "cryptography",
        "psycopg2 >= 2.0.0",
        "python-dateutil",
        "python-snappy >= 0.5",
        "requests >= 1.2.0",
    ],
    extras_require={},
    dependency_links=[],
    package_data={},
    entry_points={
        "console_scripts": [
            "pghoard = pghoard.pghoard:main",
            "pghoard_archive_sync = pghoard.archive_sync:main",
            "pghoard_create_keys = pghoard.create_keys:main",
            "pghoard_postgres_command = pghoard.postgres_command:main",
            "pghoard_restore = pghoard.restore:main",
        ],
    },
    author="Hannu Valtonen",
    author_email="hannu.valtonen@ohmu.fi",
    license="Apache 2.0",
    platforms=["POSIX", "MacOS"],
    description="PostgreSQL automatic backup/restore service daemon",
    long_description=open("README.rst").read(),
    url="https://github.com/ohmu/pghoard/",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries",
    ],
)
