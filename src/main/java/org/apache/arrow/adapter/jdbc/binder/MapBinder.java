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
        LinkedHashMap<String, String> tags = new JsonStringHashMap<>();
        while (reader.next()){
            tags.put(reader.key().readText().toString(), reader.value().readText().toString());
        }
        statement.setString(parameterIndex, tags.toString());
    }
}
