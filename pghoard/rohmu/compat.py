# rohmu - compatibility functions to support older Python 3 versions
#
# Code mostly copied from Python 3.4.3 under the Python license.

# suppress: call or simulate Pyhon 3.4 contextlib.suppress

try:
    from contextlib import \
        suppress  # pylint: disable=import-error, no-name-in-module, unused-import
except ImportError:

    class suppress:
        """Context manager to suppress specified exceptions

        After the exception is suppressed, execution proceeds with the next
        statement following the with statement.

             with suppress(FileNotFoundError):
                 os.remove(somefile)
             # Execution still resumes here if the file was already removed
        """
        def __init__(self, *exceptions):
            self._exceptions = exceptions

        def __enter__(self):
            pass

        def __exit__(self, exctype, excinst, exctb):
            # Unlike isinstance and issubclass, CPython exception handling
            # currently only looks at the concrete type hierarchy (ignoring
            # the instance and subclass checking hooks). While Guido considers
            # that a bug rather than a feature, it's a fairly hard one to fix
            # due to various internal implementation details. suppress provides
            # the simpler issubclass based semantics, rather than trying to
            # exactly reproduce the limitations of the CPython interpreter.
            #
            # See http://bugs.python.org/issue12029 for more details
            return exctype is not None and issubclass(exctype, self._exceptions)


# makedirs: call or simulate Pyhon 3.4.1 os.makedirs
# Earlier Python versions raise an error if exist_ok flag is set and the
# directory exists with different permissions.

import os
import sys

if sys.version_info >= (3, 4, 1):
    makedirs = os.makedirs
else:

    def makedirs(path, mode=0o777, exist_ok=False):
        if not exist_ok:
            return os.makedirs(path, mode)
        try:
            return os.makedirs(path, mode)
        except FileExistsError:
            pass
        return None
