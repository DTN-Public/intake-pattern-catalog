#!/usr/bin/env sh
"""
Build packages for release to Artifactory
"""

RELEASE_BRANCH=main
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [ "$CURRENT_BRANCH" = "$RELEASE_BRANCH" ]; then
    # Work around an artifactory bug related to metadata versioning
    # https://www.jfrog.com/jira/browse/RTFACT-17020
    pip install setuptools==38.5.2
    python setup.py sdist bdist_wheel --universal
fi
