package org.apache.arrow.adapter.jdbc.binder;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.JsonStringHashMap;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

public class MapBinder extends BaseColumnBinder<MapVector> {

    private final UnionMapReader reader;
    private final boolean isTextKey;
    private final boolean isTextValue;

    public MapBinder(MapVector vector) {
        this(vector, Types.VARCHAR);
    }

    public MapBinder(MapVector vector, int jdbcType) {
        super(vector, jdbcType);
        reader = vector.getReader();
        List<Field> structField = Objects.requireNonNull(vector.getField()).getChildren();
        if(structField.size() != 1){
            throw new IllegalArgumentException("Expected Struct field metadata inside Map field");
        }
        List<Field> keyValueFields = Objects.requireNonNull(structField.get(0)).getChildren();
        if(keyValueFields.size() != 2){
            throw new IllegalArgumentException("Expected two children fields inside nested Struct field in Map");
        }
        ArrowType keyType = Objects.requireNonNull(keyValueFields.get(0)).getType();
        ArrowType valueType = Objects.requireNonNull(keyValueFields.get(1)).getType();
        isTextKey = ArrowType.Utf8.INSTANCE.equals(keyType);
        isTextValue = ArrowType.Utf8.INSTANCE.equals(valueType);
    }

    @Override
    public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
        reader.setPosition(rowIndex);
        LinkedHashMap<Object, Object> tags = new JsonStringHashMap<>();
        while (reader.next()){
            Object key = reader.key().readObject();
            Object value = reader.value().readObject();
            tags.put(isTextKey && key!=null? key.toString() : key, isTextValue && value!=null ? value.toString() : value);
        }
        switch (jdbcType){
            case Types.VARCHAR:
                statement.setString(parameterIndex, tags.toString());
                break;
            case Types.OTHER:
            default:
                statement.setObject(parameterIndex, tags);
        }
    }
}
