# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

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

[v0.0.3]: https://github.com/icelake-io/icelake/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/icelake-io/icelake/compare/v0.0.1...v0.0.2
