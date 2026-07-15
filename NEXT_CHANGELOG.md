# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Throw `DatabricksSQLException` instead of an unchecked `ClassCastException` when a complex-type getter (`getArray`, `getStruct`, `getMap`) is called on a column of a different complex type.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
