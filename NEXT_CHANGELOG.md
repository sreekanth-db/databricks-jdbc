# NEXT CHANGELOG

## [Unreleased]

### Added
- Enabled direct results by default in SEA mode to improve latency for short and small queries.
- Added support for geospatial data types.
### Updated

### Fixed
- Fixed complex data type conversion issues by improving StringConverter to handle Databricks complex objects (arrays/maps/structs), JDBC arrays/structs, and generic collections.
- Fixed ComplexDataTypeParser to correctly parse ISO timestamps with T separators and timezone offsets, preventing Arrow ingestion failures.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
