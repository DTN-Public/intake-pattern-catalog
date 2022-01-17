# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Calendar Versioning](https://calver.org/).

## [Unreleased]

## [2022.1.0] - 2021-01-17

- Make `user_parameters` and emtpy dict to comply with intake 0.6.5.

## [2021.12.1] - 2021-12-10

- Unpin `boto3` in requirements.

## [2021.12.0] - 2021-12-10

- Fixed bug preventing globbing and patterns from being used together. Now globs are
passed through to driver.

## [2021.8.0] - 2021-08-25

- Added PatternCatalogTransform class, which allows for datasets to be created based on a transformation applied to PatternCatalog entries.

## [2021.7.8] - 2021-07-16

- Fix second bug to allow `simplecache::` URLs to work.

## [2021.7.7] - 2021-07-16

- Add better diff links to changelog.

## [2021.7.6] - 2021-07-16

### Fixed

- URLs with `simplecache::` or other prefix used by fsspec are now permitted.

## [2021.7.5] - 2021-07-12

### Fixed

Change get_entry and load to support `.search()` of entries.

[Unreleased]: https://bitbucket.com/dtnse/intake_pattern_catalog/branches/compare/2021.8.0..main
[2021.7.9]: https://bitbucket.com/dtnse/intake_pattern_catalog/branches/compare/2021.8.0..2021.7.8#commits
[2021.7.8]: https://bitbucket.com/dtnse/intake_pattern_catalog/branches/compare/2021.7.8..2021.7.7#commits
[2021.7.7]: https://bitbucket.com/dtnse/intake_pattern_catalog/branches/compare/2021.7.7..2021.7.6#commits
[2021.7.6]: https://bitbucket.com/dtnse/intake_pattern_catalog/branches/compare/2021.7.6..2021.7.5#commits
[2021.7.5]: https://bitbucket.com/dtnse/intake_pattern_catalog/branches/compare/2021.7.5..2021.7.4#commits
