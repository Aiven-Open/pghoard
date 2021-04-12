from setuptools import setup, find_packages
import os
import version


readme_path = os.path.join(os.path.dirname(__file__), "README.rst")
with open(readme_path, "r") as fp:
    readme_text = fp.read()


version_for_setup_py = version.get_project_version("pghoard/version.py")
version_for_setup_py = ".dev".join(version_for_setup_py.split("-", 2)[:2])


setup(
    name="pghoard",
    version=version_for_setup_py,
    zip_safe=False,
    packages=find_packages(exclude=["test"]),
    install_requires=[
        "cryptography",
        "psycopg2 >= 2.0.0",
        "pydantic",
        "python-dateutil",
        "python-snappy >= 0.5",
        "requests >= 1.2.0",
        "zstandard >= 0.11.1",
    ],
    extras_require={},
    dependency_links=[],
    package_data={},
    entry_points={
        "console_scripts": [
            "pghoard = pghoard.pghoard:main",
            "pghoard_archive_cleanup = pghoard.archive_cleanup:main",
            "pghoard_archive_sync = pghoard.archive_sync:main",
            "pghoard_create_keys = pghoard.create_keys:main",
            "pghoard_gnutaremu = pghoard.gnutaremu:main",
            "pghoard_postgres_command = pghoard.postgres_command:main",
            "pghoard_restore = pghoard.restore:main",
        ],
    },
    author="Hannu Valtonen",
    author_email="hannu.valtonen@ohmu.fi",
    license="Apache 2.0",
    platforms=["POSIX", "MacOS"],
    description="PostgreSQL automatic backup/restore service daemon",
    long_description=readme_text,
    url="https://github.com/aiven/pghoard/",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries",
    ],
)
