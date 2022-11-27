package io.avery.vinyl;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class BiIntoAPI<L, R> {
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<Mapper<L, R>> definitions = new ArrayList<>();
    
    BiIntoAPI() {} // Prevent default public constructor
    
    public <U> BiIntoAPI<L, R> leftField(Field<U> field, Function<? super L, ? extends U> mapper) {
        return field(field, (lt, rt) -> mapper.apply(lt));
    }
    
    public <U> BiIntoAPI<L, R> rightField(Field<U> field, Function<? super R, ? extends U> mapper) {
        return field(field, (lt, rt) -> mapper.apply(rt));
    }
    
    public <U> BiIntoAPI<L, R> field(Field<U> field, BiFunction<? super L, ? super R, ? extends U> mapper) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(mapper);
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        Mapper<L, R> def = new Mapper<>(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    RecordStream accept(RecordStream.Aux<L> stream,
                        Function<? super L, ? extends Stream<? extends R>> flatMapper,
                        Consumer<BiIntoAPI<L, R>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(indexByField);
        @SuppressWarnings("unchecked")
        Mapper<L, R>[] finalMappers = definitions.toArray(new Mapper[0]);
    
        // Prep the stream transformation.
        int size = finalMappers.length;
        Header nextHeader = new Header(finalIndexByField);
        Stream<Record> nextStream = stream.stream.flatMap(left -> {
            Stream<? extends R> s = flatMapper.apply(left);
            if (s == null)
                return null;
            return s.map(right -> {
                Object[] arr = new Object[size];
                for (Mapper<L, R> mapper : finalMappers)
                    mapper.accept(left, right, arr);
                return new Record(nextHeader, arr);
            });
        });
    
        return new RecordStream(nextHeader, nextStream);
    }
    
    private static class Mapper<L, R> {
        final int index;
        final BiFunction<? super L, ? super R, ?> mapper;
        
        Mapper(int index, BiFunction<? super L, ? super R, ?> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        void accept(L left, R right, Object[] arr) {
            arr[index] = mapper.apply(left, right);
        }
    }
}
