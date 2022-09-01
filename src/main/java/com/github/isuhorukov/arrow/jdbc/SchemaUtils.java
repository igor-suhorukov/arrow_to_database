package com.github.isuhorukov.arrow.jdbc;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaUtils {

    public static List<String> isCompatible(Schema baseSchema, Schema toCompareSchema){
        List<String> comparisonResult = new ArrayList<>();
        List<Field> sourceFields = baseSchema.getFields();
        List<Field> toCompareFields = toCompareSchema.getFields();
        int sourceCaseSensitiveCnt = sourceFields.stream().
                map(field -> field.getName().toLowerCase()).collect(Collectors.toSet()).size();
        if(sourceCaseSensitiveCnt!=sourceFields.size()){
            String duplicateNames = getDuplicateColumn(sourceFields);
            comparisonResult.add(
                    "Base schema contains column(s) with case sensitive name. Same name but in different case. " +
                            "Columns: "+duplicateNames);
        }
        int destinationCaseSensitiveCnt = toCompareFields.stream().
                map(field -> field.getName().toLowerCase()).collect(Collectors.toSet()).size();
        if(destinationCaseSensitiveCnt!=toCompareFields.size()){
            String duplicateNames = getDuplicateColumn(toCompareFields);
            comparisonResult.add(
                    "'to compare' schema contains column(s) with case sensitive name. Same name but in different case. "+
                            "Columns: "+duplicateNames);
        }
        if(toCompareFields.size()<sourceFields.size()){
            comparisonResult.add("Column count less in schema 'to compare'");
        } else {
            for (int fieldIdx = 0, size = sourceFields.size(); fieldIdx < size; fieldIdx++) {
                Field sourceField = sourceFields.get(fieldIdx);
                Field toCompareField = toCompareFields.get(fieldIdx);
                String sourceFieldName = sourceField.getName();
                String toCompareFieldName = toCompareField.getName();
                if(!sourceFieldName.equals(toCompareFieldName)){
                    comparisonResult.add("Difference in column "+fieldIdx+
                            " base name: "+sourceFieldName+" , 'to compare': "+toCompareFieldName);
                }
                ArrowType sourceFieldType = sourceField.getType();
                ArrowType toCompareFieldType = toCompareField.getType();
                if(!sourceFieldType.toString().equals(toCompareFieldType.toString())){
                    comparisonResult.add("Difference in column "+fieldIdx+
                            " base type:"+sourceFieldType+" , 'to compare': "+toCompareFieldType);
                }
                if(sourceFieldType.getTypeID() == ArrowType.ArrowTypeID.List ||
                        sourceFieldType.getTypeID() == ArrowType.ArrowTypeID.Map){
                    Field source = sourceField.getChildren().get(0);
                    List<Field> toCompareFieldChildren = toCompareField.getChildren();
                    Field toCompare = toCompareFieldChildren!=null && toCompareFieldChildren.size()==1 ?
                            toCompareFieldChildren.get(0) : null;
                    if(!source.equals(toCompare)){
                        comparisonResult.add("Difference in column "+fieldIdx+
                                " (Map) base type:"+source+" , 'to compare': "+toCompare);
                    }
                }
            }
        }
        return comparisonResult;
    }

    private static String getDuplicateColumn(List<Field> sourceFields) {
        List<String> columns = sourceFields.stream().map(column -> column.getName().toLowerCase()).
                                collect(Collectors.toList());
        return sourceFields.stream().map(Field::getName).
                filter(column -> Collections.frequency(columns, column.toLowerCase()) > 1).
                collect(Collectors.joining(", "));
    }
}
