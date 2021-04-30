#!/usr/bin/env python3

"""Tool for generating next version number based on Git tags.

Version number scheme is `YYYY.MM.INC0` (CalVer with incrementing micro version.)

Also includes function for tagging git with the new version and pushing to

Typically used by CI (Bitbucket Pipelines).
"""

import datetime as dt

from packaging.version import Version, parse

import intake_patterncatalog

NOW = dt.datetime.now()

CURRENT_VERSION = parse(intake_patterncatalog.__version__)


def next_version(current_version: Version = CURRENT_VERSION, now=NOW):
    if now.year == current_version.major and now.month == current_version.minor:
        micro = current_version.micro + 1
    else:
        micro = 0

    return f"{now.year}.{now.month}.{micro}"


def push_new_version():
    from git import Repo

    obj = Repo(".")
    current_version = intake_patterncatalog.__version__
    if current_version.startswith("unknown"):
        current_version = "0000.0.0"
    print(f"Current version: {current_version}")
    new_version = next_version(parse(current_version))
    print(f"New version: {new_version}")
    obj.create_tag(new_version, message="Version release")
    obj.remote().push(new_version)


if __name__ == "__main__":
    print(next_version())


def test_new_month():
    assert next_version(parse("2020.1.1"), dt.datetime(2020, 2, 3)) == "2020.2.0"


def test_this_month():
    assert next_version(parse("2020.1.1"), dt.datetime(2020, 1, 27)) == "2020.1.2"
