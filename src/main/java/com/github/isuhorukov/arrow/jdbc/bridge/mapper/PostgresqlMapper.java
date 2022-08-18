package com.github.isuhorukov.arrow.jdbc.bridge.mapper;

import java.util.Map;
import java.util.Objects;

public class PostgresqlMapper implements Mapper{
    protected static final Map<Integer, String> TYPE_MAPPING = Map.ofEntries(
                Map.entry(java.sql.Types.BIGINT, "bigint"),
                Map.entry(java.sql.Types.BOOLEAN, "boolean"),
                Map.entry(java.sql.Types.DATE, "date"),
                Map.entry(java.sql.Types.TIME, "time"),
                Map.entry(java.sql.Types.TIMESTAMP, "timestamp"),
                Map.entry(java.sql.Types.DECIMAL, "numeric"),
                Map.entry(java.sql.Types.BINARY, "bytea"),
                Map.entry(java.sql.Types.REAL, "real"),
                Map.entry(java.sql.Types.DOUBLE, "double precision"),
                Map.entry(java.sql.Types.INTEGER, "integer"),
                Map.entry(java.sql.Types.SMALLINT, "smallint"),
                Map.entry(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone"),
                Map.entry(java.sql.Types.TINYINT, "smallint"),// pguint and int1/uint1
                Map.entry(java.sql.Types.VARBINARY, "bytea"),
                Map.entry(java.sql.Types.LONGVARBINARY, "bytea"),
                Map.entry(java.sql.Types.VARCHAR, "text"),
                Map.entry(java.sql.Types.LONGVARCHAR, "text"));

    public String columnType(int jdbcType){
        return Objects.requireNonNull(TYPE_MAPPING.get(jdbcType),
                "Postgresql mapping is not found for provided java.sql.Types "+jdbcType);
    }

    public String arrayColumnType(int jdbcType){
        return String.format("%s[]", columnType(jdbcType));
    }

    public String columnDefinition(String name, String type){
        return String.format("\"%s\" %s", name, type);
    }

    public String columnName(String name){
        return String.format("\"%s\"",name);
    }

    @Override
    public String jsonType() {
        return "json";
    }
}
