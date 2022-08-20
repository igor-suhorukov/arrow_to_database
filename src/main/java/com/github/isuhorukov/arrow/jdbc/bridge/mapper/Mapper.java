package com.github.isuhorukov.arrow.jdbc.bridge.mapper;

public interface Mapper {
    public String columnType(int jdbcType);
    public String columnTypeOrNull(int jdbcType);
    public String arrayColumnType(int jdbcType);
    public String columnDefinition(String name, String type);
    public String columnName(String name);
    public String mapType();
}
