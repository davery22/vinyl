package datasalad.util;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class MapAPI<T> {
    private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
    private final List<ColumnMapper<T>> definitions = new ArrayList<>();
    
    MapAPI() {} // Prevent default public constructor
    
    public <U> MapAPI<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        ColumnMapper<T> def = new ColumnMapper<>(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    DatasetStream accept(DatasetStream.Aux<T> stream, Consumer<MapAPI<T>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Column<?>, Integer> finalIndexByColumn = Map.copyOf(indexByColumn);
        @SuppressWarnings("unchecked")
        ColumnMapper<T>[] finalMappers = definitions.toArray(ColumnMapper[]::new);
        
        // Prep the row-by-row transformation.
        int size = finalMappers.length;
        Header nextHeader = new Header(finalIndexByColumn);
        Stream<Row> nextStream = stream.stream.map(it -> {
            Object[] arr = new Object[size];
            for (ColumnMapper<T> mapper : finalMappers)
                mapper.accept(it, arr);
            return new Row(nextHeader, arr);
        });
    
        return new DatasetStream(nextHeader, nextStream);
    }
    
    Collector<T, ?, Dataset> collector(Consumer<MapAPI<T>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Column<?>, Integer> finalIndexByColumn = Map.copyOf(indexByColumn);
        @SuppressWarnings("unchecked")
        ColumnMapper<T>[] finalMappers = definitions.toArray(ColumnMapper[]::new);
        
        int size = definitions.size();
        return Collector.of(
            () -> new ArrayList<Object[]>(),
            (a, t) -> {
                Object[] arr = new Object[size];
                for (ColumnMapper<T> mapper : finalMappers)
                    mapper.accept(t, arr);
                a.add(arr);
            },
            (a, b) -> {
                a.addAll(b);
                return a;
            },
            a -> new Dataset(new Header(finalIndexByColumn), a.toArray(Object[][]::new))
        );
    }
    
    private static class ColumnMapper<T> {
        final int index;
        final Function<? super T, ?> mapper;
        
        ColumnMapper(int index, Function<? super T, ?> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        void accept(T in, Object[] arr) {
            arr[index] = mapper.apply(in);
        }
    }
}
