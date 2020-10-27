# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [7.1.5] - 2020-10-27

-  Add an option to pass a customer actor system to es client

## [7.1.4] - 2020-09-22

-  Cache JSON string inside single BulkOperation

## [7.1.3] - 2020-07-13

- Boost support in `TermQuery`, `WildcardQuery` and `PrefixQuery`
- `ConstantScore` query support

## [7.1.2] - 2020-03-19

- Fix backward compatibility with < 7.1.0

## [7.1.1] - 2020-02-25

- Support for specifying custom content type.

## [7.1.0] - 2019-12-03

### Added
- Multiple Scala Version support
- Support for both Scala 2.11 and 2.12

### Changed
- Migrated from Spray to Akka HTTP
- Gradle as a primary build / release tool
- Artifact id has a Scala version suffix
