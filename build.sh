#!/usr/bin/env sh
"""
Build packages for release to Artifactory
"""

RELEASE_BRANCH=main
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [ "$CURRENT_BRANCH" = "$RELEASE_BRANCH" ]; then
    python setup.py sdist bdist_wheel --universal
fi
