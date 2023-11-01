package com.databricks.jdbc.client;

import com.databricks.jdbc.core.DatabricksResultSet;

public interface DatabricksMetadataClient {

  /**
   * Returns information about types supported by Databricks server
   */
  DatabricksResultSet listTypeInfo();

  /**
   * Returns the list of catalogs
   */
  DatabricksResultSet listCatalogs();

  /**
   * Returns the list of schemas
   * @param catalog
   * @param schemaNamePattern
   * @return
   */
  DatabricksResultSet listSchemas(String catalog, String schemaNamePattern);

  DatabricksResultSet listTables(String catalog, String schemaNamePattern, String tableNamePattern);

  DatabricksResultSet listTableTypes();

  DatabricksResultSet listColumns(String catalog, String schemaNamePattern, String tableNamePattern, String columnNamePattern);

  DatabricksResultSet listFunctions(String catalog, String schemaNamePattern, String functionNamePattern);

  DatabricksResultSet listPrimaryKeys(String catalog, String schema, String table);
}
