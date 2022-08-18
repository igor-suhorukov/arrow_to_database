package com.github.isuhorukov.arrow.jdbc;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Objects;

public class DatasetReaderTest {
    @Test
    public void h2E2ETest() throws Exception{
        String datasetUri = Objects.requireNonNull(DatasetReaderTest.class.getResource("/data.parquet")).toExternalForm();
        String fileFormatString = "PARQUET";
        int batchSize = 1000;

        String databaseDialect = "H2";
        String driverClass = null;
        String tableName = "data";
        String jdbcUrl = "jdbc:h2:mem:testDb";
        String user = "sa";
        String password = "";
        String createTable= "regular";

        File tempFile = File.createTempFile("test_dump", "csv");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)){
            DatasetReader.copyArrowDatasetIntoTable(datasetUri, fileFormatString, batchSize,
                    databaseDialect, driverClass, tableName, jdbcUrl, user, password, createTable);
            try (Statement statement = connection.createStatement()){
                statement.executeUpdate("call CSVWRITE ( '"+tempFile.getAbsolutePath()+"', 'SELECT * FROM data order by 1 limit 10' )");
            }
            String actual = IOUtils.toString(tempFile.toURI().toURL(), StandardCharsets.UTF_8);
            String expected = IOUtils.toString(Objects.requireNonNull(DatasetReaderTest.class.getResource("/dump.csv")), StandardCharsets.UTF_8);
            Assertions.assertEquals(expected, actual);
        } finally {
            Assertions.assertTrue(tempFile.delete());
        }
    }
}
