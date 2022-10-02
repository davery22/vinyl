package datasalad.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class MapAPI<T> {
    private final DatasetStream.Aux<T> stream;
    private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
    private final List<ColumnMapper<T>> mappers = new ArrayList<>();
    
    MapAPI(DatasetStream.Aux<T> stream) {
        this.stream = stream;
    }
    
    public <U extends Comparable<U>> MapAPI<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
        int index = indexByColumn.computeIfAbsent(column, k -> mappers.size());
        ColumnMapper<T> colMapper = new ColumnMapper<>(index, mapper);
        if (index == mappers.size()) {
            mappers.add(colMapper);
        } else {
            mappers.set(index, colMapper);
        }
        return this;
    }
    
    DatasetStream accept(Consumer<MapAPI<T>> config) {
        config.accept(this);
    
        int size = mappers.size();
        Header nextHeader = new Header(indexByColumn);
        Stream<Row> nextStream = stream.stream.map(it -> {
            Comparable<?>[] arr = new Comparable[size];
            for (ColumnMapper<T> mapper : mappers) {
                mapper.accept(it, arr);
            }
            return new Row(nextHeader, arr);
        });
    
        return new DatasetStream(nextHeader, nextStream);
    }
    
    private static class ColumnMapper<T> {
        final int index;
        final Function<? super T, ? extends Comparable<?>> mapper;
        
        ColumnMapper(int index, Function<? super T, ? extends Comparable<?>> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        void accept(T in, Comparable<?>[] arr) {
            arr[index] = mapper.apply(in);
        }
    }
}
