package com.github.isuhorukov.arrow.jdbc.bridge.model;

import com.beust.jcommander.Parameter;
import com.github.isuhorukov.arrow.jdbc.bridge.DatabaseDialect;
import org.apache.arrow.dataset.file.FileFormat;

public class CliParameters {
    @Parameter(names = { "-dataset" }, required = true, description = "file:// prefexed URI of Arrow dataset  " +
            "https://arrow.apache.org/docs/python/dataset.html")
    String datasetUri;
    @Parameter(names = { "-dataset_format" }, description = "Dataset binary file format: PARQUET, ARROW_IPC, ORC, CSV, JSON")
    String fileFormatString = FileFormat.PARQUET.name();
    @Parameter(names = { "-batch_size" }, description = "Batch size to fetch and sent into DB " +
            "https://arrow.apache.org/docs/python/dataset.html#customizing-the-batch-size")
    int batchSize = 10000;

    @Parameter(names = { "-db_dialect" }, description = "Database dialect for Arrow->DB type mapping: " +
            "POSTGRESQL, H2, QUESTDB")
    String databaseDialect = DatabaseDialect.POSTGRESQL.name();

    @Parameter(names = { "-jdbc_driver" }, description = "Jdbc driver class name, provide only in case of " +
            "'No suitable driver found' issue. For example for H2 org.h2.Driver , for PostgreSQL org.postgresql.Driver")
    String jdbcDriverClass;
    @Parameter(names = { "-table_name"}, description = "Table name to import dataset into it")
    String tableName = "arrow_import";
    @Parameter(names = { "-jdbc_url"}, required = true, description = "JDBC connection URL")
    String jdbcUrl;
    @Parameter(names = { "-username"}, required = true, description = "JDBC connection user name")
    String user;
    @Parameter(names = { "-password"}, required = true, password = true, description = "JDBC password. " +
            "You can enter password in console interactive in case if not provide value after '-password' parameter")
    String password;
    @Parameter(names = { "-create_table"}, description = "If parameter provided then create table in database " +
            "before import. 'temporary' create temporary table - it can be useful for testing purpose like dry run.")
    String createTable;
    @Parameter(names = { "-insert_sql_query"},  description = "Custom SQL insert query")
    String insertSqlQuery;

    @Parameter(names = "--help", help = true)
    private boolean help;

    public String getDatasetUri() {
        return datasetUri;
    }

    public String getFileFormatString() {
        return fileFormatString;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getDatabaseDialect() {
        return databaseDialect;
    }

    public String getJdbcDriverClass() {
        return jdbcDriverClass;
    }

    public String getTableName() {
        return tableName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getCreateTable() {
        return createTable;
    }

    public String getInsertSqlQuery() {
        return insertSqlQuery;
    }

    public boolean isHelp() {
        return help;
    }
}
