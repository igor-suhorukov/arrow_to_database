package com.github.isuhorukov.arrow.jdbc.bridge.mapper;

import java.util.Map;
import java.util.Objects;

public class QuestDbMapper implements Mapper{
    protected static final Map<Integer, String> TYPE_MAPPING = Map.ofEntries(
            Map.entry(java.sql.Types.BOOLEAN, "boolean"),
            Map.entry(java.sql.Types.TINYINT, "byte"),
            Map.entry(java.sql.Types.SMALLINT, "short"),
            Map.entry(java.sql.Types.INTEGER, "int"),
            Map.entry(java.sql.Types.REAL, "float"),
            Map.entry(java.sql.Types.BIGINT, "long"),
            Map.entry(java.sql.Types.BINARY, "binary"),
            Map.entry(java.sql.Types.VARBINARY, "binary"),
            Map.entry(java.sql.Types.LONGVARBINARY, "binary"),
            Map.entry(java.sql.Types.DATE, "date"),
            Map.entry(java.sql.Types.TIMESTAMP, "timestamp"),
            Map.entry(java.sql.Types.VARCHAR, "string"),
            Map.entry(java.sql.Types.LONGVARCHAR, "string"));

    public String columnType(int jdbcType){
        return Objects.requireNonNull(TYPE_MAPPING.get(jdbcType),
                "Postgresql mapping is not found for provided java.sql.Types "+jdbcType);
    }

    @Override
    public String columnTypeOrNull(int jdbcType) {
        return TYPE_MAPPING.get(jdbcType);
    }

    public String arrayColumnType(int jdbcType){
        throw new UnsupportedOperationException("Arrays is unsupportable features in QuestDB");
    }

    public String columnDefinition(String name, String type){
        return String.format("\"%s\" %s", name, type);
    }

    public String columnName(String name){
        return String.format("\"%s\"",name);
    }

    @Override
    public String mapType() {
        return "string";
    }
}
