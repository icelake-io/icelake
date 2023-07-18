# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v0.0.8] - 2023-07-18

* feat: Initial check in of mainfest writer. by @liurenjie1024 in https://github.com/icelake-io/icelake/pull/91
* chore: allow depending on arrow version 43.0.0. by @youngsofun in https://github.com/icelake-io/icelake/pull/92

## [v0.0.7] - 2023-07-17

* feat: support data file location generator by @ZENOTME in https://github.com/icelake-io/icelake/pull/60
* docs: Add docs for all public APIs by @Xuanwo in https://github.com/icelake-io/icelake/pull/64
* deps: Allow deps within range by @Xuanwo in https://github.com/icelake-io/icelake/pull/65
* feat: Add PrimitiveValue by @Xuanwo in https://github.com/icelake-io/icelake/pull/66
* feat(in_memory): Add {Any,Struct,List,Map}Value by @Xuanwo in https://github.com/icelake-io/icelake/pull/67
* feat: Add default to in-memory structs by @Xuanwo in https://github.com/icelake-io/icelake/pull/68
* feat: Implement parser for initial-default and write-default by @Xuanwo in https://github.com/icelake-io/icelake/pull/69
* chore: Enable all-features for docs.rs by @Xuanwo in https://github.com/icelake-io/icelake/pull/70
* docs: Make rustdoc happy by @Xuanwo in https://github.com/icelake-io/icelake/pull/71
* feat: implement data writer by @ZENOTME in https://github.com/icelake-io/icelake/pull/72
* feat: support to track written size by @ZENOTME in https://github.com/icelake-io/icelake/pull/73
* feat: support task writer by @ZENOTME in https://github.com/icelake-io/icelake/pull/75
* feat: support open_with_operator by @ZENOTME in https://github.com/icelake-io/icelake/pull/76
* fix: Rename some data structures. by @liurenjie1024 in https://github.com/icelake-io/icelake/pull/77
* fix: uncorrect partition write predict by @ZENOTME in https://github.com/icelake-io/icelake/pull/78
* fix: make sure the version is valid when the table open  by @ZENOTME in https://github.com/icelake-io/icelake/pull/81
* fix: switch any::Result to crate::error::Result in table.rs by @ZENOTME in https://github.com/icelake-io/icelake/pull/82
* ci: Add partition table testdata by @Xuanwo in https://github.com/icelake-io/icelake/pull/84
* refactor: Use table format version in manifest by @liurenjie1024 in https://github.com/icelake-io/icelake/pull/85
* feat: make task_writer to use imutable reference by @ZENOTME in https://github.com/icelake-io/icelake/pull/87
* feat: Convert iceberg schema to avro schema by @liurenjie1024 in https://github.com/icelake-io/icelake/pull/88
* Bump to version 0.0.7 by @Xuanwo in https://github.com/icelake-io/icelake/pull/89

## [v0.0.6] - 2023-07-01

* feat: Add support for writing parquet by @ZENOTME in https://github.com/icelake-io/icelake/pull/50
* feat(io/parquet): Polish API by @Xuanwo in https://github.com/icelake-io/icelake/pull/53
* feat: Add icelake own error type by @Xuanwo in https://github.com/icelake-io/icelake/pull/54
* chore: Promote parquet as a mod by @Xuanwo in https://github.com/icelake-io/icelake/pull/55
* feat: Implement parquet stream by @Xuanwo in https://github.com/icelake-io/icelake/pull/59
* chore: add .vscode in .gitignore by @ZENOTME in https://github.com/icelake-io/icelake/pull/61

## [v0.0.5] - 2023-06-29

* refactor: Use arrow schema to reduce dependence tree size by @Xuanwo in https://github.com/icelake-io/icelake/pull/45
* feat: Add test data generate tools by @Xuanwo in https://github.com/icelake-io/icelake/pull/46

## [v0.0.4] - 2023-06-29

* feat: Implement convert to Arrow Schema by @Xuanwo in https://github.com/icelake-io/icelake/pull/42
* ci: Cover all features and targets by @Xuanwo in https://github.com/icelake-io/icelake/pull/43

## [v0.0.3] - 2023-06-28

* feat: Implement table load without version hint by @Xuanwo in https://github.com/icelake-io/icelake/pull/34

## [v0.0.2] - 2023-06-27

* feat: add open table interface and add an example to read table by @xudong963 in https://github.com/icelake-io/icelake/pull/27
* chore: remove Cargo.lock by @TennyZhuang in https://github.com/icelake-io/icelake/pull/30
* feat: Add current data files support by @Xuanwo in https://github.com/icelake-io/icelake/pull/31
* Bump to version 0.0.2 by @Xuanwo in https://github.com/icelake-io/icelake/pull/32

## v0.0.1 - 2023-06-17

* feat: Add project layout by @Xuanwo in https://github.com/icelake-io/icelake/pull/2
* feat: Add iceberg schema data types by @Xuanwo in https://github.com/icelake-io/icelake/pull/3
* feat: Add Schema V2 types by @Xuanwo in https://github.com/icelake-io/icelake/pull/5
* feat: Implement parse json into SchemaV2 by @Xuanwo in https://github.com/icelake-io/icelake/pull/6
* refactor: Use move instead of clone from reference by @Xuanwo in https://github.com/icelake-io/icelake/pull/7
* feat: Add Transform, Partition and Sorting by @Xuanwo in https://github.com/icelake-io/icelake/pull/8
* feat: Implement parse partition spec by @Xuanwo in https://github.com/icelake-io/icelake/pull/9
* feat: Implement parse from sort_order by @Xuanwo in https://github.com/icelake-io/icelake/pull/10
* feat: Add manifest related types by @Xuanwo in https://github.com/icelake-io/icelake/pull/11
* feat: Implement manifest parse by @Xuanwo in https://github.com/icelake-io/icelake/pull/12
* refactor: Remove version suffix by @Xuanwo in https://github.com/icelake-io/icelake/pull/13
* refactor: Re-organize the project layout by @Xuanwo in https://github.com/icelake-io/icelake/pull/14
* fix: parse_manifest_list should return lists instead by @Xuanwo in https://github.com/icelake-io/icelake/pull/15
* feat: Add in-memory layout for snapshot by @Xuanwo in https://github.com/icelake-io/icelake/pull/16
* feat: Implement parse_snapshot by @Xuanwo in https://github.com/icelake-io/icelake/pull/17
* feat: Implement version_hint for Table by @Xuanwo in https://github.com/icelake-io/icelake/pull/18
* feat: Add in-memory types for table metadata by @Xuanwo in https://github.com/icelake-io/icelake/pull/19
* feat: Implement parse of table metadata by @Xuanwo in https://github.com/icelake-io/icelake/pull/20
* feat: Load table metadata from storage by @Xuanwo in https://github.com/icelake-io/icelake/pull/21
* ci: Add publish CI by @Xuanwo in https://github.com/icelake-io/icelake/pull/23

[v0.0.8]: https://github.com/icelake-io/icelake/compare/v0.0.8...v0.0.7
[v0.0.7]: https://github.com/icelake-io/icelake/compare/v0.0.7...v0.0.6
[v0.0.6]: https://github.com/icelake-io/icelake/compare/v0.0.5...v0.0.6
[v0.0.5]: https://github.com/icelake-io/icelake/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/icelake-io/icelake/compare/v0.0.3...v0.0.4
[v0.0.3]: https://github.com/icelake-io/icelake/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/icelake-io/icelake/compare/v0.0.1...v0.0.2
