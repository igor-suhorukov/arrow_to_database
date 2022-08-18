package com.github.isuhorukov.arrow.jdbc.bridge.model;

import java.util.Collections;
import java.util.List;

public class TableMetadata {
    private String name;
    private List<String> columnNames;
    private List<String> columnDefinitions;
    private List<String> placeholders;

    public TableMetadata(String name, List<String> columnNames, List<String> columnDefinitions,
                         List<String> placeholders) {
        this.name = name;
        this.columnNames = columnNames;
        this.columnDefinitions = columnDefinitions;
        this.placeholders = placeholders;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    public List<String> getColumnDefinitions() {
        return Collections.unmodifiableList(columnDefinitions);
    }

    public String getColumns(){
        return String.join(", ", columnNames);
    }

    public String getDefinitions(){
        return String.join(", ", columnDefinitions);
    }

    public String getParameterPlaceholders(){
       return String.join(", ", placeholders);
    }
}
