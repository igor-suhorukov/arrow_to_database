package com.github.isuhorukov.arrow.jdbc.bridge.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TableMetadata {
    private String name;
    private List<String> columnNames;
    private List<String> columnDefinitions;
    private List<String> placeholders;
    private String comment;
    private Map<String, String> columnComments;

    public TableMetadata(String name, List<String> columnNames, List<String> columnDefinitions,
                         List<String> placeholders, String comment, Map<String, String> columnComments) {
        this.name = Objects.requireNonNull(name, "name");
        this.columnNames = Objects.requireNonNull(columnNames,"columnNames");
        this.columnDefinitions = Objects.requireNonNull(columnDefinitions,"columnDefinitions");
        this.placeholders = Objects.requireNonNull(placeholders,"placeholders");
        this.comment = comment;
        this.columnComments = columnComments!=null ? columnComments : Collections.emptyMap();
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

    public String getComment() {
        return comment;
    }

    public Map<String, String> getColumnComments() {
        return Collections.unmodifiableMap(columnComments);
    }
}
