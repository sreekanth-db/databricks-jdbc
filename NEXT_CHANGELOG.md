# NEXT CHANGELOG

## [Unreleased]

### Added
- **Query Tags support**: Added ability to attach key-value tags to SQL queries for analytical purposes. Tags follow format `"key1:value1,key2:value2"` and are supported via both URL parameters (`jdbc:databricks://host;QUERY_TAGS=team:marketing,dashboard:abc123`) and connection properties (`properties.put("QUERY_TAGS", "team:engineering,project:pipeline")`). Tags appear in `system.query.history` table and work with both Thrift and SEA protocols.

### Updated

### Fixed
- Fixed `DatabricksUCVolumeClient` delete to skip file path validation and remove redundant dependency on `VolumeOperationAllowedLocalPaths`.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
