package datasalad.util;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class MapAPI<T> {
    private final DatasetStream.Aux<T> stream;
    private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
    private final List<ColumnMapper<T>> definitions = new ArrayList<>();
    
    MapAPI(DatasetStream.Aux<T> stream) {
        this.stream = stream;
    }
    
    public <U extends Comparable<U>> MapAPI<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        ColumnMapper<T> def = new ColumnMapper<>(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    DatasetStream accept(Consumer<MapAPI<T>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Column<?>, Integer> finalIndexByColumn = Map.copyOf(indexByColumn);
        List<ColumnMapper<T>> finalMappers = List.copyOf(definitions);
        
        // Prep the row-by-row transformation.
        int size = definitions.size();
        Header nextHeader = new Header(finalIndexByColumn);
        Stream<Row> nextStream = stream.stream.map(it -> {
            Comparable<?>[] arr = new Comparable[size];
            for (ColumnMapper<T> mapper : finalMappers)
                mapper.accept(it, arr);
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
