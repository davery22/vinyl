package vinyl;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class MapAPI<T> {
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<FieldMapper<T>> definitions = new ArrayList<>();
    
    MapAPI() {} // Prevent default public constructor
    
    public <U> MapAPI<T> field(Field<U> field, Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(mapper);
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        FieldMapper<T> def = new FieldMapper<>(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    RecordStream accept(RecordStream.Aux<T> stream, Consumer<MapAPI<T>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(indexByField);
        @SuppressWarnings("unchecked")
        FieldMapper<T>[] finalMappers = definitions.toArray(new FieldMapper[0]);
        
        // Prep the stream transformation.
        int size = finalMappers.length;
        Header nextHeader = new Header(finalIndexByField);
        Stream<Record> nextStream = stream.stream.map(it -> {
            Object[] arr = new Object[size];
            for (FieldMapper<T> mapper : finalMappers)
                mapper.accept(it, arr);
            return new Record(nextHeader, arr);
        });
    
        return new RecordStream(nextHeader, nextStream);
    }
    
    Collector<T, ?, RecordSet> collector(Consumer<MapAPI<T>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(indexByField);
        @SuppressWarnings("unchecked")
        FieldMapper<T>[] finalMappers = definitions.toArray(new FieldMapper[0]);
        
        int size = definitions.size();
        return Collector.of(
            () -> new ArrayList<Object[]>(),
            (a, t) -> {
                Object[] arr = new Object[size];
                for (FieldMapper<T> mapper : finalMappers)
                    mapper.accept(t, arr);
                a.add(arr);
            },
            (a, b) -> {
                a.addAll(b);
                return a;
            },
            a -> new RecordSet(new Header(finalIndexByField), a.toArray(new Object[0][]))
        );
    }
    
    private static class FieldMapper<T> {
        final int index;
        final Function<? super T, ?> mapper;
        
        FieldMapper(int index, Function<? super T, ?> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        void accept(T in, Object[] arr) {
            arr[index] = mapper.apply(in);
        }
    }
}
