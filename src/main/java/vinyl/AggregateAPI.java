/*
 * MIT License
 *
 * Copyright (c) 2022 Daniel Avery
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package vinyl;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static vinyl.Utils.cast;
import static vinyl.Utils.tempField;

/**
 * A configurator used to define an aggregate operation on a {@link RecordStream record-stream}.
 *
 * <p>An aggregate operation may define zero or more keys, that together group the input stream into non-overlapping
 * partitions. The keys may be retained as output fields, or discarded. Each partition will capture one or more input
 * records, and produce exactly one output record. In the special case that no keys are used and the stream is empty,
 * the default partition will capture no input records, but will still produce exactly one output record.
 *
 * <p>An aggregate operation may define fields as aggregate functions. An aggregate function is applied per-partition,
 * and returns one value for the partition, which becomes the value of the field on the output record for that
 * partition.
 *
 * <p>Fields defined on the configurator (or any sub-configurators) become fields on a resultant {@link Header header}.
 * The header fields are in order of definition on the configurator(s). A field may be redefined on the configurator(s),
 * in which case its definition is replaced, but its original position in the header is retained.
 *
 * @see RecordStream#aggregate
 */
public class AggregateAPI {
    private final RecordStream stream;
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<Object> definitions = new ArrayList<>();
    
    AggregateAPI(RecordStream stream) {
        this.stream = stream;
    }
    
    /**
     * Defines a key as the lookup of the given field on each input record.
     *
     * @param field the field
     * @return this configurator
     * @throws NoSuchElementException if the stream header does not contain the given field
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public AggregateAPI key(Field<?> field) {
        FieldPin pin = stream.header.pin(field);
        return keyHelper(true, tempField(), record -> record.get(pin));
    }
    
    /**
     * Defines a key as the application of the given function to each input record.
     *
     * @param mapper a function to be applied to each input record
     * @return this configurator
     */
    public AggregateAPI key(Function<? super Record, ?> mapper) {
        Objects.requireNonNull(mapper);
        return keyHelper(true, tempField(), mapper);
    }
    
    /**
     * Defines (or redefines) the given field as a key that looks up the same field on each input record.
     *
     * @param field the field
     * @return this configurator
     * @throws NoSuchElementException if the stream header does not contain the given field
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public AggregateAPI keyField(Field<?> field) {
        FieldPin pin = stream.header.pin(field);
        return keyHelper(false, field, record -> record.get(pin));
    }
    
    /**
     * Defines (or redefines) the given field as a key that applies the given function to each input record.
     *
     * @param field the field
     * @param mapper a function to be applied to each input record
     * @return this configurator
     * @param <T> the value type of the field
     */
    public <T> AggregateAPI keyField(Field<T> field, Function<? super Record, ? extends T> mapper) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(mapper);
        return keyHelper(false, field, mapper);
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
    
    /**
     * Defines (or redefines) the given field as an aggregate over input records, using the given collector.
     *
     * @param field the field
     * @param collector the collector that describes the aggregate operation over input records
     * @return this configurator
     * @param <T> the value type of the field
     */
    public <T> AggregateAPI aggField(Field<T> field, Collector<? super Record, ?, ? extends T> collector) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(collector);
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        AggRecordCollector def = new AggRecordCollector(field, collector);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    /**
     * Defines (or redefines) each of the given fields as keys that look up the same field on each input record.
     *
     * @param fields the fields
     * @return this configurator
     * @throws NoSuchElementException if the stream header does not contain a given field
     */
    public AggregateAPI keyFields(Field<?>... fields) {
        for (Field<?> field : fields)
            keyField(field);
        return this;
    }
    
    /**
     * Defines keys from each of the given fields, as the lookup of the same field on each input record.
     *
     * @param fields the fields
     * @return this configurator
     * @throws NoSuchElementException if the stream header does not contain a given field
     */
    public AggregateAPI keys(Field<?>... fields) {
        for (Field<?> field : fields)
            key(field);
        return this;
    }
    
    /**
     * Configures a sub-configurator, that may define (or redefine) keys in terms of the result of applying the given
     * function to each input record. If no keys are defined by the sub-configurator, possibly due to later field
     * redefinitions, the entire sub-configurator is discarded.
     *
     * @param mapper a function to be applied to each input record
     * @param config a consumer of the sub-configurator
     * @return this configurator
     * @param <T> the result type of the function
     */
    public <T> AggregateAPI keys(Function<? super Record, ? extends T> mapper, Consumer<Keys<T>> config) {
        Objects.requireNonNull(mapper);
        config.accept(new Keys<>(mapper));
        return this;
    }
    
    /**
     * Configures a sub-configurator, that may define (or redefine) fields in terms of the result of aggregating the
     * input records using the given collector. If no fields are defined by the sub-configurator, possibly due to later
     * field redefinitions, the entire sub-configurator is discarded.
     *
     * @param collector the collector that describes the aggregate operation over input records
     * @param config a consumer of the sub-configurator
     * @return this configurator
     * @param <T> the result type of the aggregation
     */
    public <T> AggregateAPI aggs(Collector<? super Record, ?, ? extends T> collector, Consumer<Aggs<T>> config) {
        Objects.requireNonNull(collector);
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
                Keys<?> parent = def.addToParent();
                if (parent != null)
                    finalMappers.add(parent);
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
                Aggs<?> parent = def.addToParent();
                if (parent != null)
                    finalCollectors.add(parent);
                def.localIndex = aggIndexes.size();
                aggIndexes.add(j);
                indexByField.put(def.field, j++);
            }
        }
        
        // Prep the stream transformation.
        int size = indexByField.size();
        Header nextHeader = new Header(new HashMap<>(indexByField));
        Collector<Record, ?, List<?>> downstream = collectorFor(finalCollectors, aggIndexes.size());
        Stream<Record> nextStream;
        
        // TODO: Optimize special cases:
        //  1. (DONE) No keys
        //  2. One key
        //  3. No aggs
        //  4. One agg
    
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
        // Chain a no-op, so that the stream will throw if re-used after this call.
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
    
    /**
     * A sub-configurator used to define keys of an aggregate operation that depend on a common intermediate result.
     *
     * @param <T> the intermediate result type
     */
    public class Keys<T> extends Mapper {
        final Function<? super Record, ? extends T> mapper;
        final List<KeyObjectMapper> children = new ArrayList<>();
        
        Keys(Function<? super Record, ? extends T> mapper) {
            this.mapper = mapper;
        }
    
        /**
         * Defines a key as the application of the given function to this sub-configurator's intermediate result.
         *
         * @param mapper a function to be applied to this sub-configurator's intermediate result
         * @return this configurator
         */
        public Keys<T> key(Function<? super T, ?> mapper) {
            Objects.requireNonNull(mapper);
            return keyHelper(true, tempField(), mapper);
        }
    
        /**
         * Defines (or redefines) a field as a key that applies the given function to this sub-configurator's
         * intermediate result.
         *
         * @param field the field
         * @param mapper a function to be applied to this sub-configurator's intermediate result
         * @return this configurator
         * @param <U> the value type of the field
         */
        public <U> Keys<T> keyField(Field<U> field, Function<? super T, ? extends U> mapper) {
            Objects.requireNonNull(field);
            Objects.requireNonNull(mapper);
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
            
            Keys<T> addToParent() {
                boolean isParentUnseen = Keys.this.children.isEmpty();
                Keys.this.children.add(this);
                return isParentUnseen ? Keys.this : null;
            }
        }
    }
    
    /**
     * A sub-configurator used to define fields of an aggregate operation that depend on a common intermediate
     * aggregation result.
     *
     * @param <T> the intermediate result type
     */
    public class Aggs<T> extends CollectorBox {
        final Collector<? super Record, ?, ? extends T> collector;
        final List<AggObjectMapper> children = new ArrayList<>();
        
        Aggs(Collector<? super Record, ?, ? extends T> collector) {
            this.collector = collector;
        }
    
        /**
         * Defines (or redefines) a field as the application of the given function to this sub-configurator's
         * intermediate aggregation result.
         *
         * @param field the field
         * @param mapper a function to be applied to this sub-configurator's intermediate aggregation result
         * @return this configurator
         * @param <U> the value type of the field
         */
        public <U> Aggs<T> aggField(Field<U> field, Function<? super T, ? extends U> mapper) {
            Objects.requireNonNull(field);
            Objects.requireNonNull(mapper);
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
            
            Aggs<T> addToParent() {
                boolean isParentUnseen = Aggs.this.children.isEmpty();
                Aggs.this.children.add(this);
                return isParentUnseen ? Aggs.this : null;
            }
        }
    }
}
