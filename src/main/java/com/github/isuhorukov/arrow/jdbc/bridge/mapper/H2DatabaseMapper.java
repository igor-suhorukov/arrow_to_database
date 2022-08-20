package com.github.isuhorukov.arrow.jdbc.bridge.mapper;

import java.util.Map;
import java.util.Objects;

public class H2DatabaseMapper implements Mapper{
    protected static final Map<Integer, String> TYPE_MAPPING = Map.ofEntries(
            Map.entry(java.sql.Types.BIGINT, "long"),
            Map.entry(java.sql.Types.INTEGER, "int"),
            Map.entry(java.sql.Types.BOOLEAN, "boolean"),
            Map.entry(java.sql.Types.REAL, "real"),
            Map.entry(java.sql.Types.DOUBLE, "double"),
            Map.entry(java.sql.Types.BINARY, "binary varying"),
            Map.entry(java.sql.Types.VARBINARY, "binary varying"),
            Map.entry(java.sql.Types.LONGVARBINARY, "binary varying"),
            Map.entry(java.sql.Types.DATE, "date"),
            Map.entry(java.sql.Types.TIME, "time"),
            Map.entry(java.sql.Types.TIMESTAMP, "timestamp"),
            Map.entry(java.sql.Types.DECIMAL, "numeric"),
            Map.entry(java.sql.Types.SMALLINT, "smallint"),
            Map.entry(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone"),
            Map.entry(java.sql.Types.TINYINT, "TINYINT"),
            Map.entry(java.sql.Types.VARCHAR, "VARCHAR"),
            Map.entry(java.sql.Types.LONGVARCHAR, "VARCHAR"));

    public String columnType(int jdbcType){
        return Objects.requireNonNull(TYPE_MAPPING.get(jdbcType),
                "Postgresql mapping is not found for provided java.sql.Types "+jdbcType);
    }

    @Override
    public String columnTypeOrNull(int jdbcType) {
        return TYPE_MAPPING.get(jdbcType);
    }

    public String arrayColumnType(int jdbcType){
        return String.format("%s array", columnType(jdbcType));
    }

    public String columnDefinition(String name, String type){
        return String.format("\"%s\" %s", name, type);
    }

    public String columnName(String name){
        return String.format("\"%s\"",name);
    }

    @Override
    public String mapType() {
        return "VARCHAR";
    }
}
