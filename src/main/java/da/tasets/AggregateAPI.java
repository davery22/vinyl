package da.tasets;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static da.tasets.Utils.cast;
import static da.tasets.Utils.tempField;

public class AggregateAPI {
    private final RecordStream stream;
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<Object> definitions = new ArrayList<>();
    
    AggregateAPI(RecordStream stream) {
        this.stream = stream;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public AggregateAPI key(Field<?> field) {
        FieldPin pin = new FieldPin(field, stream.header.indexOf(field));
        return keyHelper(true, tempField(), record -> record.get(pin));
    }
    
    public AggregateAPI key(Function<? super Record, ?> mapper) {
        return keyHelper(true, tempField(), mapper);
    }
    
    public AggregateAPI keyField(Field<?> field) {
        return keyHelper(false, field);
    }
    
    public <T> AggregateAPI keyField(Field<T> field, Function<? super Record, ? extends T> mapper) {
        return keyHelper(false, field, mapper);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private AggregateAPI keyHelper(boolean isTemp, Field<?> field) {
        FieldPin pin = new FieldPin(field, stream.header.indexOf(field));
        return keyHelper(isTemp, field, record -> record.get(pin));
    }
    
    private <T> AggregateAPI keyHelper(boolean isTemp, Field<?> field, Function<? super Record, ? extends T> mapper) {
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        KeyRecordMapper def = new KeyRecordMapper(isTemp, field, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    public <T, A> AggregateAPI aggField(Field<T> field, Collector<? super Record, A, ? extends T> collector) {
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        AggRecordCollector def = new AggRecordCollector(field, collector);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    public AggregateAPI keyFields(Field<?>... fields) {
        for (Field<?> field : fields)
            keyField(field);
        return this;
    }
    
    public AggregateAPI keys(Field<?>... fields) {
        for (Field<?> field : fields)
            key(field);
        return this;
    }
    
    public <T> AggregateAPI keys(Function<? super Record, ? extends T> mapper, Consumer<Keys<T>> config) {
        config.accept(new Keys<>(mapper));
        return this;
    }
    
    public <T> AggregateAPI aggs(Collector<? super Record, ?, ? extends T> collector, Consumer<Aggs<T>> config) {
        config.accept(new Aggs<>(collector));
        return this;
    }
    
    RecordStream accept(Consumer<AggregateAPI> config) {
        config.accept(this);
    
        // General idea:
        //  - combine key functions into one classifier that creates a List (with hash/equals) of key values for a record
        //  - combine agg functions into one collector that creates a List of agg values for a record
        //  - create a groupingBy collector from classifier and downstream collector
        //  - stream Map entries, create record from key values and agg values
        //
        // Handling of temp keys:
        //  Remove temp keys from final indexByField, and adjust other indexes around that.
        //  Mark temp keys so that we can check and skip adding them to the final records.
        
        List<Integer> keyIndexes = new ArrayList<>();
        List<Integer> aggIndexes = new ArrayList<>();
        List<Mapper> finalMappers = new ArrayList<>();
        List<CollectorBox> finalCollectors = new ArrayList<>();
        Set<Object> seenParents = new HashSet<>();
        boolean hasTempKeys = false;
        
        // i: original index, j: adjusted index (after removing temps)
        for (int i = 0, j = 0; i < definitions.size(); i++) {
            Object definition = definitions.get(i);
            if (definition instanceof KeyRecordMapper) {
                KeyRecordMapper def = (KeyRecordMapper) definition;
                finalMappers.add(def);
                def.localIndex = keyIndexes.size();
                if (def.isTemp) {
                    hasTempKeys = true;
                    keyIndexes.add(-1); // Mark for skipping
                    indexByField.remove(def.field);
                } else {
                    keyIndexes.add(j);
                    indexByField.put(def.field, j++);
                }
            } else if (definition instanceof AggRecordCollector) {
                AggRecordCollector def = (AggRecordCollector) definition;
                finalCollectors.add(def);
                def.localIndex = aggIndexes.size();
                aggIndexes.add(j);
                indexByField.put(def.field, j++);
            } else if (definition instanceof Keys.KeyObjectMapper) {
                Keys<?>.KeyObjectMapper def = (Keys<?>.KeyObjectMapper) definition;
                def.addToParent();
                if (seenParents.add(def.parent()))
                    finalMappers.add(def.parent());
                def.localIndex = keyIndexes.size();
                if (def.isTemp) {
                    hasTempKeys = true;
                    keyIndexes.add(-1); // Mark for skipping
                    indexByField.remove(def.field);
                } else {
                    keyIndexes.add(j);
                    indexByField.put(def.field, j++);
                }
            } else {
                assert definition instanceof Aggs.AggObjectMapper;
                Aggs<?>.AggObjectMapper def = (Aggs<?>.AggObjectMapper) definition;
                def.addToParent();
                if (seenParents.add(def.parent()))
                    finalCollectors.add(def.parent());
                def.localIndex = aggIndexes.size();
                aggIndexes.add(j);
                indexByField.put(def.field, j++);
            }
        }
        
        // Prep the stream transformation.
        int size = indexByField.size();
        Header nextHeader = new Header(Map.copyOf(indexByField));
        Collector<Record, ?, List<?>> downstream = collectorFor(finalCollectors, aggIndexes.size());
        Stream<Record> nextStream;
    
        // Optimization: Inline and simplify lazyCollect() + mapToDataset(), since this is trusted code.
        // Optimization: Skip grouping if there are no keys.
        if (keyIndexes.isEmpty()) {
            nextStream = lazyCollectingStream(downstream)
                .map(aggs -> {
                    Object[] arr = new Object[size];
                    for (int i = 0; i < aggIndexes.size(); i++)
                        arr[aggIndexes.get(i)] = aggs.get(i);
                    return new Record(nextHeader, arr);
                });
        } else {
            Function<Record, List<?>> classifier = classifierFor(finalMappers, keyIndexes.size());
            Collector<Record, ?, Map<List<?>, List<?>>> collector = Collectors.groupingBy(classifier, downstream);
            nextStream = lazyCollectingStream(collector)
                .flatMap(map -> map.entrySet().stream())
                .map(
                    hasTempKeys ? e -> {
                        Object[] arr = new Object[size];
                        List<?> keys = e.getKey();
                        List<?> aggs = e.getValue();
                        for (int i = 0; i < keyIndexes.size(); i++)
                            if (keyIndexes.get(i) != -1) // Skip if marked
                                arr[keyIndexes.get(i)] = keys.get(i);
                        for (int i = 0; i < aggIndexes.size(); i++)
                            arr[aggIndexes.get(i)] = aggs.get(i);
                        return new Record(nextHeader, arr);
                    } : e -> {
                        Object[] arr = new Object[size];
                        List<?> keys = e.getKey();
                        List<?> aggs = e.getValue();
                        for (int i = 0; i < keyIndexes.size(); i++)
                            arr[keyIndexes.get(i)] = keys.get(i);
                        for (int i = 0; i < aggIndexes.size(); i++)
                            arr[aggIndexes.get(i)] = aggs.get(i);
                        return new Record(nextHeader, arr);
                    });
        }
        
        return new RecordStream(nextHeader, nextStream);
    }
    
    private <T> Stream<T> lazyCollectingStream(Collector<Record, ?, T> collector) {
        Stream<Record> s = stream.stream.peek(it -> {});
        return StreamSupport.stream(
            () -> Collections.singleton(s.collect(collector)).spliterator(),
            Spliterator.SIZED | Spliterator.SUBSIZED | Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.ORDERED,
            s.isParallel()
        ).onClose(s::close);
    }
    
    private Function<Record, List<?>> classifierFor(List<Mapper> mappers, int keysSize) {
        return record -> {
            Object[] arr = new Object[keysSize];
            for (Mapper mapper : mappers)
                mapper.accept(record, arr);
            return Arrays.asList(arr); // convert to List for equals/hashCode
        };
    }
    
    private Collector<Record, ?, List<?>> collectorFor(List<CollectorBox> collectorBoxes, int aggsSize) {
        int size = collectorBoxes.size();
        Supplier<?>[] suppliers = new Supplier[size];
        BiConsumer<?, ?>[] accumulators = new BiConsumer[size];
        BinaryOperator<?>[] combiners = new BinaryOperator[size];
        Function<?, ?>[] finishers = new Function[size];
        
        for (int i = 0; i < size; i++) {
            Collector<?, ?, ?> collector = collectorBoxes.get(i).collector();
            suppliers[i] = collector.supplier();
            accumulators[i] = collector.accumulator();
            combiners[i] = collector.combiner();
            finishers[i] = collector.finisher();
        }
        
        return Collector.of(
            () -> {
                Object[] arr = new Object[size];
                for (int i = 0; i < size; i++)
                    arr[i] = suppliers[i].get();
                return arr;
            },
            (a, t) -> {
                for (int i = 0; i < size; i++)
                    accumulators[i].accept(cast(a[i]), cast(t));
            },
            (a, b) -> {
                for (int i = 0; i < size; i++)
                    a[i] = combiners[i].apply(cast(a[i]), cast(b[i]));
                return a;
            },
            a -> {
                Object[] arr = new Object[aggsSize];
                for (int i = 0; i < size; i++) {
                    Object it = finishers[i].apply(cast(a[i]));
                    collectorBoxes.get(i).accept(it, arr);
                }
                return Arrays.asList(arr);
            }
        );
    }
    
    private abstract static class Mapper {
        abstract void accept(Record record, Object[] arr);
    }
    
    private abstract static class CollectorBox {
        abstract Collector<? super Record, ?, ?> collector();
        abstract void accept(Object obj, Object[] arr);
    }
    
    private static class KeyRecordMapper extends Mapper {
        final boolean isTemp;
        final Field<?> field;
        final Function<? super Record, ?> mapper;
        int localIndex;
    
        KeyRecordMapper(boolean isTemp, Field<?> field, Function<? super Record, ?> mapper) {
            this.isTemp = isTemp;
            this.field = field;
            this.mapper = mapper;
        }
        
        @Override
        void accept(Record record, Object[] arr) {
            arr[localIndex] = mapper.apply(record);
        }
    }
    
    private static class AggRecordCollector extends CollectorBox {
        final Field<?> field;
        final Collector<? super Record, ?, ?> collector;
        int localIndex;
    
        AggRecordCollector(Field<?> field, Collector<? super Record, ?, ?> collector) {
            this.field = field;
            this.collector = collector;
        }
        
        @Override
        Collector<? super Record, ?, ?> collector() {
            return collector;
        }
        
        @Override
        void accept(Object obj, Object[] arr) {
            arr[localIndex] = obj;
        }
    }
    
    public class Keys<T> extends Mapper {
        final Function<? super Record, ? extends T> mapper;
        final List<KeyObjectMapper> children = new ArrayList<>();
        
        Keys(Function<? super Record, ? extends T> mapper) {
            this.mapper = mapper;
        }
    
        public Keys<T> key(Function<? super T, ?> mapper) {
            return keyHelper(true, tempField(), mapper);
        }
        
        public Keys<T> key(Field<?> field, Function<? super T, ?> mapper) {
            return keyHelper(true, field, mapper);
        }
        
        public <U> Keys<T> keyField(Field<U> field, Function<? super T, ? extends U> mapper) {
            return keyHelper(false, field, mapper);
        }
        
        private <U> Keys<T> keyHelper(boolean isTemp, Field<?> field, Function<? super T, ? extends U> mapper) {
            int index = indexByField.computeIfAbsent(field, k -> definitions.size());
            KeyObjectMapper def = new KeyObjectMapper(isTemp, field, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        @Override
        void accept(Record record, Object[] arr) {
            T obj = mapper.apply(record);
            children.forEach(child -> child.accept(obj, arr));
        }
        
        private class KeyObjectMapper {
            final boolean isTemp;
            final Field<?> field;
            final Function<? super T, ?> mapper;
            int localIndex;
        
            KeyObjectMapper(boolean isTemp, Field<?> field, Function<? super T, ?> mapper) {
                this.isTemp = isTemp;
                this.field = field;
                this.mapper = mapper;
            }
            
            void accept(T in, Object[] arr) {
                arr[localIndex] = mapper.apply(in);
            }
            
            void addToParent() {
                Keys.this.children.add(this);
            }
            
            Keys<T> parent() {
                return Keys.this;
            }
        }
    }
    
    public class Aggs<T> extends CollectorBox {
        final Collector<? super Record, ?, ? extends T> collector;
        final List<AggObjectMapper> children = new ArrayList<>();
        
        Aggs(Collector<? super Record, ?, ? extends T> collector) {
            this.collector = collector;
        }
        
        public <U> Aggs<T> aggField(Field<U> field, Function<? super T, ? extends U> mapper) {
            int index = indexByField.computeIfAbsent(field, k -> definitions.size());
            AggObjectMapper def = new AggObjectMapper(field, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        @Override
        Collector<? super Record, ?, ?> collector() {
            return collector;
        }
        
        @Override
        void accept(Object obj, Object[] arr) {
            @SuppressWarnings("unchecked")
            T in = (T) obj;
            children.forEach(child -> child.accept(in, arr));
        }
        
        private class AggObjectMapper {
            final Field<?> field;
            final Function<? super T, ?> mapper;
            int localIndex;
    
            AggObjectMapper(Field<?> field, Function<? super T, ?> mapper) {
                this.field = field;
                this.mapper = mapper;
            }
            
            void accept(T in, Object[] arr) {
                arr[localIndex] = mapper.apply(in);
            }
            
            void addToParent() {
                Aggs.this.children.add(this);
            }
            
            Aggs<T> parent() {
                return Aggs.this;
            }
        }
    }
}
