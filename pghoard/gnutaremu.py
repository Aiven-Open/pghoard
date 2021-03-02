#!/usr/bin/python3
import argparse
import fnmatch
import os
import re
import sys
import tarfile


class GnuTarEmulator:
    """Provides minimal set of tar processing functionality with interface
    identical to that of GNU tar. Only parameters that are required by PGHoard
    are supported."""
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-x", "--extract", help="Extract a file", action="store_true", required=True)
        parser.add_argument("-f", "--file", help="Specify the file to extract, - for stdin", type=str, required=True)
        parser.add_argument("-C", "--directory", help="Target directory for extraction", type=str)
        parser.add_argument("-P", "--absolute-names", help="Don't strip leading / from file names", action="store_true")
        parser.add_argument(
            "--keep-directory-symlink",
            help="Follow symlinks to directories when extracting from the archive",
            action="store_true"
        )
        parser.add_argument("--exclude", help="Exclude file matching given patter", type=str, action="append")
        parser.add_argument("--transform", help="Transform file name", type=str, action="append")
        self.args = parser.parse_args()
        self.substitutions = self._process_transform_arguments()

    @classmethod
    def makedirs(cls, path):
        try:
            return os.makedirs(path, 0o777)
        except FileExistsError:
            pass
        return None

    def run(self):
        return self._extract(self._open_input_file())

    def _extract(self, file):
        paths = []
        with tarfile.open(fileobj=file, mode="r|") as tar:
            for tarinfo in tar:
                target_name = self._build_target_name(tarinfo.name)
                if not target_name:
                    continue

                if not self.args.keep_directory_symlink:
                    # _build_target_name prefixed path with directory name but if absolute
                    # paths are allowed the path might be outside of target directory
                    if self.args.directory and target_name.startswith(self.args.directory):
                        path = self.args.directory
                        relative_path = target_name[len(self.args.directory):].lstrip(os.sep)
                    elif target_name.startswith(os.sep):
                        path = os.sep
                        relative_path = target_name.lstrip(os.sep)
                    else:
                        path = "."
                        relative_path = target_name
                    parts = relative_path.split(os.sep)
                    for part in parts:
                        path = os.path.join(path, part)
                        if os.path.islink(path):
                            os.unlink(path)
                            break  # only remove first symlink

                if tarinfo.isdir():
                    paths.append([target_name, tarinfo])
                    self.makedirs(target_name)
                elif tarinfo.isreg():
                    target_dir = os.path.dirname(target_name)
                    if not os.path.exists(target_dir):
                        self.makedirs(target_dir)
                    tar.makefile(tarinfo, target_name)
                    paths.append([target_name, tarinfo])
                elif tarinfo.issym():
                    os.symlink(tarinfo.linkname, target_name)
                else:
                    raise Exception("Unrecognized file type for file {!r} in tar".format(tarinfo.name))

        for target_name, tarinfo in paths:
            tar.chmod(tarinfo, target_name)
            tar.utime(tarinfo, target_name)

        return 0

    def _build_target_name(self, source_name):
        name = source_name
        if self._should_exclude(name):
            return None
        name = self._transform_name(name)
        if not name:
            return None
        if not self.args.absolute_names:
            name = name.lstrip(os.sep)
            if f"..{os.sep}" in name or name.endswith(".."):
                return None
        if not name.startswith(os.sep) and self.args.directory:
            name = os.path.join(self.args.directory, name)
        return name

    def _open_input_file(self):
        if self.args.file == "-":
            return sys.stdin.buffer
        else:
            return open(self.args.file, "rb")

    def _process_transform_arguments(self):
        if not self.args.transform:
            return {}
        return dict(SedStatementParser(statement).parse() for statement in self.args.transform)

    def _should_exclude(self, filename):
        if not self.args.exclude:
            return False
        # GNU tar exclude matching is hungry and it matches any subpart of the name
        parts = filename.split(os.sep)
        for exclude_pattern in self.args.exclude:
            if any(part for part in parts if fnmatch.fnmatch(part, exclude_pattern)):
                return True
        return False

    def _transform_name(self, name):
        for pattern, substitution in self.substitutions.items():
            name = pattern.sub(substitution, name)
        return name


class SedStatementParser:
    def __init__(self, statement):
        self.statement = statement

    def parse(self):
        if not self.statement.startswith("s"):
            raise Exception("Transform statement must start with 's' {!r}".format(self.statement))
        statement = self.statement[1:]
        separator = statement[0]
        if not statement.endswith(separator):
            raise Exception("Transform statement must end with separator {!r} ({!r})".format(separator, statement))
        tokens = self.tokenize_string(statement[1:-1], separator)
        if len(tokens) != 2:
            raise Exception("Bad transform statement, must have search and replace expressions {!r}".format(statement))
        search = tokens[0]
        replace = tokens[1]
        search = self.reverse_escaping(search)
        return re.compile(search), replace

    @classmethod
    def reverse_escaping(cls, string):
        # tar --transform has reverse behavior for "{}()+?|" than Python regexes (characters
        # must be escaped to invoke their special behavior, unescaped ones are handled literally)
        found_escape = False
        output = ""
        for c in string:
            if c == "\\":
                if found_escape:
                    output += "\\\\"
                    found_escape = False
                else:
                    found_escape = True
            elif found_escape:
                if c not in "{}()+?|":
                    output += "\\"
                output += c
                found_escape = False
            elif c in "{}()+?|":
                output += "\\" + c
            else:
                output += c
        return output

    @classmethod
    def tokenize_string(cls, string, separator):
        """Split string with given separator unless the separator is escaped with backslash"""
        results = []
        token = ""
        found_escape = False
        for c in string:
            if found_escape:
                if c == separator:
                    token += separator
                else:
                    token += "\\" + c
                found_escape = False
                continue
            if c == "\\":
                found_escape = True
            elif c == separator:
                results.append(token)
                token = ""
            else:
                token += c

        results.append(token)
        return results


def main():
    return GnuTarEmulator().run()


if __name__ == "__main__":
    sys.exit(main() or 0)
