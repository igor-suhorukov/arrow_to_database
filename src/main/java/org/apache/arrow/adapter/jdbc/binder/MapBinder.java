package org.apache.arrow.adapter.jdbc.binder;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.util.JsonStringHashMap;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;

public class MapBinder extends BaseColumnBinder<MapVector> {

    private UnionMapReader reader;

    public MapBinder(MapVector vector) {
        this(vector, Types.VARCHAR);
    }

    public MapBinder(MapVector vector, int jdbcType) {
        super(vector, jdbcType);
        reader = vector.getReader();
    }

    @Override
    public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
        reader.setPosition(rowIndex);
        LinkedHashMap<Object, Object> tags = new JsonStringHashMap<>();
        while (reader.next()){
            tags.put(reader.key().readObject(), reader.value().readObject());
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
