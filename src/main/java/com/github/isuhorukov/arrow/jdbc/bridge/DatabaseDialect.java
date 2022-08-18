package com.github.isuhorukov.arrow.jdbc.bridge;

import com.github.isuhorukov.arrow.jdbc.bridge.mapper.H2DatabaseMapper;
import com.github.isuhorukov.arrow.jdbc.bridge.mapper.Mapper;
import com.github.isuhorukov.arrow.jdbc.bridge.mapper.PostgresqlMapper;

public enum DatabaseDialect {

    POSTGRESQL(new PostgresqlMapper()), H2(new H2DatabaseMapper());

    private Mapper mapper;

    DatabaseDialect(Mapper mapper) {
        this.mapper = mapper;
    }

    public Mapper getMapper() {
        return mapper;
    }
}
