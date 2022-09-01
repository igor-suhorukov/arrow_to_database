package com.github.isuhorukov.arrow.jdbc;

import com.github.isuhorukov.arrow.jdbc.bridge.DatabaseDialect;
import com.github.isuhorukov.arrow.jdbc.bridge.mapper.Mapper;
import com.github.isuhorukov.arrow.jdbc.bridge.model.TableMetadata;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.adapter.jdbc.binder.ColumnBinder;
import org.apache.arrow.adapter.jdbc.binder.ColumnBinderArrowTypeVisitor;
import org.apache.arrow.adapter.jdbc.binder.MapBinder;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

public class DatasetReader {
    private final static Logger LOGGER = LoggerFactory.getLogger(DatasetReader.class);

    public static final String CREATE_TABLE = "create table ";
    public static final String CREATE_TEMP_TABLE = "create temporary table ";
    public static final String TEMPORARY = "temporary";
    public static final String COMMENT = "comment";

    public static void copyArrowDatasetIntoTable(String datasetUri, String fileFormatString, int batchSize,
                                  String databaseDialectString, String driverClass, String tableName,
                                  String jdbcUrl, String user, String password,
                                  String createTable, String insertSqlQuery) throws Exception {
        FileFormat fileFormat = FileFormat.valueOf(fileFormatString);
        DatabaseDialect databaseDialect = DatabaseDialect.valueOf(databaseDialectString);
        TableMetadata tableMetadata = readArrowMetadataAndMapToDbTypes(datasetUri,
                                                        fileFormat, 1, databaseDialect, tableName);

        if(driverClass !=null){
            Class.forName(driverClass);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)){
            connection.setAutoCommit(false);
            if(createTable !=null && !createTable.isEmpty()){
                try (Statement statement = connection.createStatement()){
                    String tableDDL = (TEMPORARY.equalsIgnoreCase(createTable)? CREATE_TEMP_TABLE : CREATE_TABLE)
                            + tableMetadata.getName()+"("+tableMetadata.getDefinitions()+")";
                    LOGGER.info("Create table to import data. DDL {}", tableDDL);
                    statement.executeUpdate(tableDDL);
                }
            }
            try (PreparedStatement preparedStatement = connection.prepareStatement(getInsertQuery(insertSqlQuery, tableMetadata))){
                copyArrowDatasetIntoPreparedStatement(batchSize, fileFormat, datasetUri, preparedStatement);
            } catch (Exception e){
                connection.rollback();
                throw e;
            }
            connection.commit();
        }
    }

    private static String getInsertQuery(String insertSqlQuery, TableMetadata tableMetadata) {
        String insertQuery;
        if (insertSqlQuery == null) {
            insertQuery = "insert into " + tableMetadata.getName() + "(" + tableMetadata.getColumns() +
                    ") values(" + tableMetadata.getParameterPlaceholders() + ")";
            LOGGER.info("Generated 'insert' query from Apache Arrow schema: {}", insertQuery);
        } else {
            insertQuery = insertSqlQuery;
        }
        return insertQuery;
    }

    private static void copyArrowDatasetIntoPreparedStatement(int batchSize, FileFormat fileFormat,
                                              String datasetUri, PreparedStatement preparedStatement) throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            FileSystemDatasetFactory factory = new FileSystemDatasetFactory(allocator,
                                                        NativeMemoryPool.getDefault(), fileFormat, datasetUri);
            final Dataset dataset = factory.finish();
            ScanOptions options = new ScanOptions(batchSize);
            final Scanner scanner = dataset.newScan(options);
            try {
                AtomicLong totalRows = new AtomicLong();
                StreamSupport.stream(scanner.scan().spliterator(), false).forEach(scanTask -> {
                    long rowCount=0;
                    try (ArrowReader reader = scanTask.execute()) {
                        while (reader.loadNextBatch()) {
                            VectorSchemaRoot root = reader.getVectorSchemaRoot();
                            JdbcParameterBinder binder = JdbcParameterBinder
                                                            .builder(preparedStatement, root).bindAll().build();
                            while (binder.next()) {
                                preparedStatement.addBatch();
                                rowCount++;
                            }
                            preparedStatement.executeBatch();
                        }
                        totalRows.addAndGet(rowCount);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                LOGGER.info("Total rows imported: {}", totalRows.get());

            } finally {
                AutoCloseables.close(scanner, dataset);
            }
        }
    }

    public static TableMetadata readArrowMetadataAndMapToDbTypes(String datasetUri, FileFormat fileFormat, int batchSize,
                                                                 DatabaseDialect databaseDialect, String tableName) {
        try (BufferAllocator allocator = new RootAllocator()) {
            FileSystemDatasetFactory factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                                                        fileFormat, datasetUri);
            final Dataset dataset = factory.finish();
            ScanOptions options = new ScanOptions(batchSize);
            final Scanner scanner = dataset.newScan(options);
            Schema schema = scanner.schema();
            return getTableMetadataFromSchema(schema, databaseDialect, tableName);
        }
    }

    public static TableMetadata getTableMetadataFromSchema(Schema schema, DatabaseDialect databaseDialect,
                                                           String tableName) {
        try (BufferAllocator allocator = new RootAllocator()) {
            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
                return getTableMetadata(vectorSchemaRoot, databaseDialect, tableName);
            }
        }
    }

    private static TableMetadata getTableMetadata(VectorSchemaRoot root, DatabaseDialect databaseDialect,
                                                  String tableName) {
        List<String> columnNames = new ArrayList<>();
        List<String> columnDefinition = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        Mapper databaseDialectMapper = databaseDialect.getMapper();
        LinkedHashMap<String, String> columnComments = new LinkedHashMap<>();
        for(int columnIdx = 0; columnIdx < root.getFieldVectors().size(); ++columnIdx) {
            FieldVector vector = root.getVector(columnIdx);
            Field field = vector.getField();
            ColumnBinder binder = field.getType().accept(
                                    new ColumnBinderArrowTypeVisitor(vector, null));
            String name = vector.getName();
            String columnName = databaseDialectMapper.columnName(name);
            columnNames.add(columnName);
            String comment = field.getMetadata().get(COMMENT);
            if(comment!=null && !comment.isEmpty()){
                columnComments.put(columnName, comment);
            }
            if (binder.getJdbcType()== Types.ARRAY){
                FieldVector listVector = ((ListVector) vector).getDataVector();
                ColumnBinder scalarType = listVector.getField().getType().accept(
                                            new ColumnBinderArrowTypeVisitor(listVector, null));
                columnDefinition.add(databaseDialectMapper.columnDefinition(
                                            name, databaseDialectMapper.arrayColumnType(scalarType.getJdbcType())));
                placeholders.add("?");
            } else {
                String columnType;
                if(binder instanceof MapBinder){
                    columnType = databaseDialectMapper.mapType();
                    if(!columnType.equals(databaseDialectMapper.columnTypeOrNull(binder.getJdbcType()))){
                        placeholders.add("CAST(? AS "+columnType+")");
                    } else {
                        placeholders.add("?");
                    }
                } else {
                    columnType = databaseDialectMapper.columnType(binder.getJdbcType());
                    placeholders.add("?");
                }
                columnDefinition.add(databaseDialectMapper.columnDefinition(name, columnType));
            }
        }
        return new TableMetadata(tableName, columnNames, columnDefinition, placeholders, root.getSchema().getCustomMetadata().get("comment"), !columnComments.isEmpty() ? columnComments : null);
    }

    public static String[] readArrowMetadata(String datasetUri, FileFormat fileFormat, int batchSize,
                                             DatabaseDialect databaseDialect, String tableName) {
        TableMetadata tableMetadata = readArrowMetadataAndMapToDbTypes(datasetUri, fileFormat,
                batchSize, databaseDialect, tableName);
        return new String[]{ tableMetadata.getDefinitions(), tableMetadata.getColumns(), tableMetadata.getName()};
    }
}
