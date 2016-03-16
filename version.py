"""
automatically maintains the latest git tag + revision info in a python file

"""

import imp
import os
import subprocess


def get_project_version(version_file):
    version_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), version_file)
    try:
        module = imp.load_source("verfile", version_file)
        file_ver = module.__version__
    except:
        file_ver = None

    try:
        git_out = subprocess.check_output(["git", "describe", "--always"])
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass
    else:
        git_ver = git_out.splitlines()[0].strip().decode("utf-8")
        if git_ver and ((git_ver != file_ver) or not file_ver):
            with open(version_file, "w") as fp:
                fp.write("__version__ = '{}'\n".format(git_ver))
            return git_ver

    if not file_ver:
        raise Exception("version not available from git or from file {!r}".format(version_file))

    return file_ver

if __name__ == "__main__":
    import sys
    get_project_version(sys.argv[1])
