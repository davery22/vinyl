package datasalad.util;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static datasalad.util.UnsafeUtils.cast;

public class AggregateAPI {
    private final DatasetStream stream;
    private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
    private final List<Object> definitions = new ArrayList<>();
    
    AggregateAPI(DatasetStream stream) {
        this.stream = stream;
    }
    
    public AggregateAPI tempKey(Column<?> column) {
        return keyHelper(true, column);
    }
    
    public <T> AggregateAPI tempKey(Column<T> column, Function<? super Row, ? extends T> mapper) {
        return keyHelper(true, column, mapper);
    }
    
    public AggregateAPI key(Column<?> column) {
        return keyHelper(false, column);
    }
    
    public <T> AggregateAPI key(Column<T> column, Function<? super Row, ? extends T> mapper) {
        return keyHelper(false, column, mapper);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private AggregateAPI keyHelper(boolean isTemp, Column<?> column) {
        Locator locator = new Locator(column, stream.header.indexOf(column));
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        KeyRowMapper def = new KeyRowMapper(isTemp, column, row -> row.get(locator));
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    private <T> AggregateAPI keyHelper(boolean isTemp, Column<T> column, Function<? super Row, ? extends T> mapper) {
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        KeyRowMapper def = new KeyRowMapper(isTemp, column, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    public <T, A> AggregateAPI agg(Column<T> column, Collector<? super Row, A, ? extends T> collector) {
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        AggRowCollector def = new AggRowCollector(column, collector);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    public <T> AggregateAPI keys(Function<? super Row, ? extends T> mapper, Consumer<Keys<T>> config) {
        config.accept(new Keys<>(mapper));
        return this;
    }
    
    public <T> AggregateAPI aggs(Collector<? super Row, ?, ? extends T> collector, Consumer<Aggs<T>> config) {
        config.accept(new Aggs<>(collector));
        return this;
    }
    
    DatasetStream accept(Consumer<AggregateAPI> config) {
        config.accept(this);
    
        // General idea:
        //  - combine key functions into one classifier that creates a List (with hash/equals) of key values for a row
        //  - combine agg functions into one collector that creates a List of agg values for a row
        //  - create a groupingBy collector from classifier and downstream collector
        //  - stream Map entries, create row from key values and agg values
        //
        // Handling of temp keys:
        //  Remove temp keys from final indexByColumn, and adjust other indexes around that.
        //  Mark temp keys so that we can check and skip adding them to the final rows.
        
        List<Integer> keyIndexes = new ArrayList<>();
        List<Integer> aggIndexes = new ArrayList<>();
        List<Mapper> finalMappers = new ArrayList<>();
        List<CollectorBox> finalCollectors = new ArrayList<>();
        Set<Object> seenParents = new HashSet<>();
        boolean hasTempKeys = false;
        
        // i: original index, j: adjusted index (after removing temps)
        for (int i = 0, j = 0; i < definitions.size(); i++) {
            Object definition = definitions.get(i);
            if (definition instanceof KeyRowMapper) {
                KeyRowMapper def = (KeyRowMapper) definition;
                finalMappers.add(def);
                def.localIndex = keyIndexes.size();
                if (def.isTemp) {
                    hasTempKeys = true;
                    keyIndexes.add(-1); // Mark for skipping
                    indexByColumn.remove(def.column);
                } else {
                    keyIndexes.add(j);
                    indexByColumn.put(def.column, j++);
                }
            } else if (definition instanceof AggRowCollector) {
                AggRowCollector def = (AggRowCollector) definition;
                finalCollectors.add(def);
                def.localIndex = aggIndexes.size();
                aggIndexes.add(j);
                indexByColumn.put(def.column, j++);
            } else if (definition instanceof Keys.KeyObjMapper) {
                Keys<?>.KeyObjMapper def = (Keys<?>.KeyObjMapper) definition;
                def.addToParent();
                if (seenParents.add(def.parent()))
                    finalMappers.add(def.parent());
                def.localIndex = keyIndexes.size();
                if (def.isTemp) {
                    hasTempKeys = true;
                    keyIndexes.add(-1); // Mark for skipping
                    indexByColumn.remove(def.column);
                } else {
                    keyIndexes.add(j);
                    indexByColumn.put(def.column, j++);
                }
            } else {
                assert definition instanceof Aggs.AggObjMapper;
                Aggs<?>.AggObjMapper def = (Aggs<?>.AggObjMapper) definition;
                def.addToParent();
                if (seenParents.add(def.parent()))
                    finalCollectors.add(def.parent());
                def.localIndex = aggIndexes.size();
                aggIndexes.add(j);
                indexByColumn.put(def.column, j++);
            }
        }
        
        // Prep the row-by-row transformation.
        int size = indexByColumn.size();
        Header nextHeader = new Header(Map.copyOf(indexByColumn));
        Collector<Row, ?, List<?>> downstream = collectorFor(finalCollectors, aggIndexes.size());
        Stream<Row> nextStream;
    
        // Optimization: Inline and simplify lazyCollect() + mapToDataset(), since this is trusted code.
        // Optimization: Skip grouping if there are no keys.
        if (keyIndexes.isEmpty()) {
            nextStream = lazyCollectingStream(downstream)
                .map(aggs -> {
                    Object[] arr = new Object[size];
                    for (int i = 0; i < aggIndexes.size(); i++)
                        arr[aggIndexes.get(i)] = aggs.get(i);
                    return new Row(nextHeader, arr);
                });
        } else {
            Function<Row, List<?>> classifier = classifierFor(finalMappers, keyIndexes.size());
            Collector<Row, ?, Map<List<?>, List<?>>> collector = Collectors.groupingBy(classifier, downstream);
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
                        return new Row(nextHeader, arr);
                    } : e -> {
                        Object[] arr = new Object[size];
                        List<?> keys = e.getKey();
                        List<?> aggs = e.getValue();
                        for (int i = 0; i < keyIndexes.size(); i++)
                            arr[keyIndexes.get(i)] = keys.get(i);
                        for (int i = 0; i < aggIndexes.size(); i++)
                            arr[aggIndexes.get(i)] = aggs.get(i);
                        return new Row(nextHeader, arr);
                    });
        }
        
        return new DatasetStream(nextHeader, nextStream);
    }
    
    private <T> Stream<T> lazyCollectingStream(Collector<Row, ?, T> collector) {
        Stream<Row> s = stream.stream.peek(it -> {});
        return StreamSupport.stream(
            () -> Collections.singleton(s.collect(collector)).spliterator(),
            Spliterator.SIZED | Spliterator.SUBSIZED | Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.ORDERED,
            s.isParallel()
        ).onClose(s::close);
    }
    
    private Function<Row, List<?>> classifierFor(List<Mapper> mappers, int keysSize) {
        return row -> {
            Object[] arr = new Object[keysSize];
            for (Mapper mapper : mappers)
                mapper.accept(row, arr);
            return Arrays.asList(arr); // convert to List for equals/hashCode
        };
    }
    
    private Collector<Row, ?, List<?>> collectorFor(List<CollectorBox> collectorBoxes, int aggsSize) {
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
        abstract void accept(Row row, Object[] arr);
    }
    
    private abstract static class CollectorBox {
        abstract Collector<? super Row, ?, ?> collector();
        abstract void accept(Object obj, Object[] arr);
    }
    
    private static class KeyRowMapper extends Mapper {
        final boolean isTemp;
        final Column<?> column;
        final Function<? super Row, ?> mapper;
        int localIndex;
    
        KeyRowMapper(boolean isTemp, Column<?> column, Function<? super Row, ?> mapper) {
            this.isTemp = isTemp;
            this.column = column;
            this.mapper = mapper;
        }
        
        @Override
        void accept(Row row, Object[] arr) {
            arr[localIndex] = mapper.apply(row);
        }
    }
    
    private static class AggRowCollector extends CollectorBox {
        final Column<?> column;
        final Collector<? super Row, ?, ?> collector;
        int localIndex;
    
        AggRowCollector(Column<?> column, Collector<? super Row, ?, ?> collector) {
            this.column = column;
            this.collector = collector;
        }
        
        @Override
        Collector<? super Row, ?, ?> collector() {
            return collector;
        }
        
        @Override
        void accept(Object obj, Object[] arr) {
            arr[localIndex] = obj;
        }
    }
    
    public class Keys<T> extends Mapper {
        final Function<? super Row, ? extends T> mapper;
        final List<KeyObjMapper> children = new ArrayList<>();
        
        Keys(Function<? super Row, ? extends T> mapper) {
            this.mapper = mapper;
        }
    
        public <U> Keys<T> tempKey(Column<U> column, Function<? super T, ? extends U> mapper) {
            return keyHelper(true, column, mapper);
        }
        
        public <U> Keys<T> key(Column<U> column, Function<? super T, ? extends U> mapper) {
            return keyHelper(false, column, mapper);
        }
        
        private <U> Keys<T> keyHelper(boolean isTemp, Column<U> column, Function<? super T, ? extends U> mapper) {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            KeyObjMapper def = new KeyObjMapper(isTemp, column, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        @Override
        void accept(Row row, Object[] arr) {
            T obj = mapper.apply(row);
            children.forEach(child -> child.accept(obj, arr));
        }
        
        private class KeyObjMapper {
            final boolean isTemp;
            final Column<?> column;
            final Function<? super T, ?> mapper;
            int localIndex;
        
            KeyObjMapper(boolean isTemp, Column<?> column, Function<? super T, ?> mapper) {
                this.isTemp = isTemp;
                this.column = column;
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
        final Collector<? super Row, ?, ? extends T> collector;
        final List<AggObjMapper> children = new ArrayList<>();
        
        Aggs(Collector<? super Row, ?, ? extends T> collector) {
            this.collector = collector;
        }
        
        public <U> Aggs<T> agg(Column<U> column, Function<? super T, ? extends U> mapper) {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            AggObjMapper def = new AggObjMapper(column, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        @Override
        Collector<? super Row, ?, ?> collector() {
            return collector;
        }
        
        @Override
        void accept(Object obj, Object[] arr) {
            @SuppressWarnings("unchecked")
            T in = (T) obj;
            children.forEach(child -> child.accept(in, arr));
        }
        
        private class AggObjMapper {
            final Column<?> column;
            final Function<? super T, ?> mapper;
            int localIndex;
    
            AggObjMapper(Column<?> column, Function<? super T, ?> mapper) {
                this.column = column;
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
