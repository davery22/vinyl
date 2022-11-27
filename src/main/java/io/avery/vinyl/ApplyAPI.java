package io.avery.vinyl;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ApplyAPI<R> {
    private final ApplyType type;
    private final RecordStream left;
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<Mapper<R>> definitions = new ArrayList<>();
    
    enum ApplyType {
        CROSS,
        OUTER
    }
    
    ApplyAPI(ApplyType type, RecordStream left) {
        this.type = type;
        this.left = left;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ApplyAPI<R> leftField(Field<?> field) {
        FieldPin<?> pin = left.header.pin(field);
        return field((Field) field, (lt, rt) -> lt.get(pin));
    }
    
    public ApplyAPI<R> leftFields(Field<?>... fields) {
        for (Field<?> field : fields)
            leftField(field);
        return this;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ApplyAPI<R> leftAllFields() {
        Field<?>[] fields = left.header.fields;
        for (int i = 0; i < fields.length; i++) {
            FieldPin<?> pin = new FieldPin<>(fields[i], i);
            return field((Field) fields[i], (lt, rt) -> lt.get(pin));
        }
        return this;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ApplyAPI<R> leftAllFieldsExcept(Field<?>... excluded) {
        Set<Field<?>> excludedSet = Set.of(excluded);
        Field<?>[] fields = left.header.fields;
        for (int i = 0; i < fields.length; i++)
            if (!excludedSet.contains(fields[i])) {
                FieldPin<?> pin = new FieldPin<>(fields[i], i);
                return field((Field) fields[i], (lt, rt) -> lt.get(pin));
            }
        return this;
    }
    
    public <U> ApplyAPI<R> leftField(Field<U> field, Function<? super Record, ? extends U> mapper) {
        return field(field, (lt, rt) -> mapper.apply(lt));
    }
    
    public <U> ApplyAPI<R> rightField(Field<U> field, Function<? super R, ? extends U> mapper) {
        return field(field, (lt, rt) -> mapper.apply(rt));
    }
    
    public <U> ApplyAPI<R> field(Field<U> field, BiFunction<? super Record, ? super R, ? extends U> mapper) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(mapper);
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        Mapper<R> def = new Mapper<>(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    RecordStream accept(Function<? super Record, ? extends Stream<? extends R>> flatMapper,
                        Consumer<ApplyAPI<R>> config) {
        config.accept(this);
        
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(indexByField);
        @SuppressWarnings("unchecked")
        Mapper<R>[] finalMappers = definitions.toArray(new Mapper[0]);
        
        // Prep the stream transformation.
        int size = finalMappers.length;
        Header nextHeader = new Header(finalIndexByField);
        Stream<Record> nextStream;
        if (type == ApplyType.CROSS)
            nextStream = left.stream.flatMap(left -> {
                Stream<? extends R> s = flatMapper.apply(left);
                if (s == null)
                    return null;
                return s.map(right -> {
                    Object[] arr = new Object[size];
                    for (Mapper<R> mapper : finalMappers)
                        mapper.accept(left, right, arr);
                    return new Record(nextHeader, arr);
                });
            });
        else // type == ApplyType.OUTER
            nextStream = left.stream.flatMap(left -> {
                Stream<? extends R> s = flatMapper.apply(left);
                if (s == null)
                    s = Stream.of((R) null);
                else {
                    class Box { R item; }
                    Box box = new Box();
                    Spliterator<? extends R> spliter = s.spliterator();
                    if (!spliter.tryAdvance(el -> box.item = el))
                        s = Stream.of((R) null).onClose(s::close);
                    else
                        s = Stream.concat(Stream.of(box.item), StreamSupport.stream(spliter, false)).onClose(s::close);
                }
                return s.map(right -> {
                    Object[] arr = new Object[size];
                    for (Mapper<R> mapper : finalMappers)
                        mapper.accept(left, right, arr);
                    return new Record(nextHeader, arr);
                });
            });
        
        return new RecordStream(nextHeader, nextStream);
    }
    
    private static class Mapper<R> {
        final int index;
        final BiFunction<? super Record, ? super R, ?> mapper;
        
        Mapper(int index, BiFunction<? super Record, ? super R, ?> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        void accept(Record left, R right, Object[] arr) {
            arr[index] = mapper.apply(left, right);
        }
    }
}
