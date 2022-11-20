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

package io.avery.vinyl;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        return keyHelper(true, Utils.tempField(), record -> record.get(pin));
    }
    
    /**
     * Defines a key as the application of the given function to each input record.
     *
     * @param mapper a function to be applied to each input record
     * @return this configurator
     */
    public AggregateAPI key(Function<? super Record, ?> mapper) {
        Objects.requireNonNull(mapper);
        return keyHelper(true, Utils.tempField(), mapper);
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
     * Defines (or redefines) the given field as the application of the given function to each <em>output</em> record.
     * The field may reference other output fields on the same record, including other post-fields, as long as it does
     * not reference itself (including transitively). If a reference cycle is detected during evaluation, an
     * {@link IllegalStateException} will be thrown.
     *
     * @param field the field
     * @param mapper a function to apply to each <em>output</em> record
     * @return this configurator
     * @param <T> the value type of the field
     */
    public <T> AggregateAPI postField(Field<T> field, Function<? super Record, ? extends T> mapper) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(mapper);
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        PostMapper def = new PostMapper(field, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
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
    public <T> AggregateAPI keys(Function<? super Record, ? extends T> mapper,
                                 Consumer<Keys<T>> config) {
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
    public <T> AggregateAPI aggs(Collector<? super Record, ?, ? extends T> collector,
                                 Consumer<Aggs<T>> config) {
        Objects.requireNonNull(collector);
        config.accept(new Aggs<>(collector));
        return this;
    }
    
    /**
     * Configures a sub-configurator, that may define (or redefine) fields as aggregates over values emitted to those
     * fields by the given routing function. If no fields are defined by the sub-configurator, possibly due to later
     * field redefinitions, the entire sub-configurator is discarded.
     *
     * <p>If the routing function emits to a field that is not defined by the sub-configurator, there is no effect. If
     * the routing function emits multiple values to the same field, the field aggregates each value.
     *
     * <p>Routing is more general than, but can efficiently express, a SQL "pivot" operation. SQL pivot syntax is
     * dialect-specific and not universally supported, but an example query might look something like:
     *
     * <pre>{@code
     *     SELECT * FROM (
     *         SELECT VendorId, EmployeeId, PurchaseAmount
     *         FROM Purchases
     *     )
     *     PIVOT (
     *         AVG(PurchaseAmount)
     *         FOR EmployeeId IN (250 Emp1, 251 Emp2, 256 Emp3, 257 Emp4, 260 Emp5)
     *     )
     * }</pre>
     *
     * <p>Analogous Java code (with no attempt to remove duplication) might look like:
     *
     * <pre>{@code
     *     RecordStream stream = purchasesStream
     *         .aggregate(aggregate -> aggregate
     *             .keyField(vendorId)
     *             .<Long>route(
     *                 (record, sink) -> {
     *                     Long amount = record.get(purchaseAmount);
     *                     switch (record.get(employeeId)) {
     *                         case 250: sink.accept(emp1, amount); break;
     *                         case 251: sink.accept(emp2, amount); break;
     *                         case 256: sink.accept(emp3, amount); break;
     *                         case 257: sink.accept(emp4, amount); break;
     *                         case 260: sink.accept(emp5, amount); break;
     *                     }
     *                 },
     *                 route -> route
     *                     .aggField(emp1, Collectors.averagingLong(amount -> amount))
     *                     .aggField(emp2, Collectors.averagingLong(amount -> amount))
     *                     .aggField(emp3, Collectors.averagingLong(amount -> amount))
     *                     .aggField(emp4, Collectors.averagingLong(amount -> amount))
     *                     .aggField(emp5, Collectors.averagingLong(amount -> amount))
     *             )
     *         );
     * }</pre>
     *
     * <p>Or, with some refactoring:
     *
     * <pre>{@code
     *     Map<Integer, Field<Double>> empById = Map.of(250, emp1, 251, emp2, 256, emp3, 257, emp4, 260, emp5);
     *
     *     RecordStream stream = purchasesStream
     *         .aggregate(aggregate -> aggregate
     *             .keyField(vendorId)
     *             .<Long>route(
     *                 (record, sink) -> sink.accept(empById.get(record.get(employeeId)), record.get(purchaseAmount)),
     *                 route -> empById.forEach((id, emp) -> route.aggField(emp, Collectors.averagingLong(amount -> amount)))
     *             )
     *         );
     * }</pre>
     *
     * @param router the routing function
     * @param config a consumer of the sub-configurator
     * @return this configurator
     * @param <T> the type of values emitted by the routing function
     */
    public <T> AggregateAPI route(BiConsumer<? super Record, ? super BiConsumer<Field<?>, T>> router,
                                  Consumer<Route<T>> config) {
        Objects.requireNonNull(router);
        config.accept(new Route<>(router));
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
        List<PostMapper> finalPostMappers = new ArrayList<>();
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
            }
            else if (definition instanceof PostMapper) {
                PostMapper def = (PostMapper) definition;
                finalPostMappers.add(def);
                def.index = j;
                indexByField.put(def.field, j++);
            }
            else if (definition instanceof AggRecordCollector) {
                AggRecordCollector def = (AggRecordCollector) definition;
                finalCollectors.add(def);
                def.localIndex = aggIndexes.size();
                aggIndexes.add(j);
                indexByField.put(def.field, j++);
            }
            else if (definition instanceof Keys.KeyObjectMapper) {
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
            }
            else if (definition instanceof Aggs.AggObjectMapper) {
                Aggs<?>.AggObjectMapper def = (Aggs<?>.AggObjectMapper) definition;
                Aggs<?> parent = def.addToParent();
                if (parent != null)
                    finalCollectors.add(parent);
                def.localIndex = aggIndexes.size();
                aggIndexes.add(j);
                indexByField.put(def.field, j++);
            }
            else {
                assert definition instanceof Route.RoutedObjectCollector;
                Route<?>.RoutedObjectCollector def = (Route<?>.RoutedObjectCollector) definition;
                Route<?> parent = def.addToParent();
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
    
        if (!finalPostMappers.isEmpty())
            nextStream = nextStream.peek(out -> {
                Record.RecursiveRecord record = new Record.RecursiveRecord(nextHeader, out.values);
                for (PostMapper mapper : finalPostMappers)
                    out.values[mapper.index] = new Record.Redirect(mapper.mapper);
                for (PostMapper mapper : finalPostMappers)
                    record.eval(mapper.index);
                record.isDone = true; // Ensure proper behavior for fields that may have captured the record itself
            });
        
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
                    accumulators[i].accept(Utils.cast(a[i]), Utils.cast(t));
            },
            (a, b) -> {
                for (int i = 0; i < size; i++)
                    a[i] = combiners[i].apply(Utils.cast(a[i]), Utils.cast(b[i]));
                return a;
            },
            a -> {
                Object[] arr = new Object[aggsSize];
                for (int i = 0; i < size; i++) {
                    Object it = finishers[i].apply(Utils.cast(a[i]));
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
    
    private static class PostMapper {
        final Field<?> field;
        final Function<? super Record, ?> mapper;
        int index;
        
        PostMapper(Field<?> field, Function<? super Record, ?> mapper) {
            this.field = field;
            this.mapper = mapper;
        }
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
     * @see AggregateAPI#keys(Function, Consumer)
     * @param <T> the intermediate result type
     */
    public class Keys<T> extends Mapper {
        final Function<? super Record, ? extends T> mapper;
        final List<KeyObjectMapper> children = new ArrayList<>();
        
        Keys(Function<? super Record, ? extends T> mapper) {
            this.mapper = mapper;
        }
        
        /**
         * Returns the parent of this sub-configurator.
         *
         * @return the parent of this sub-configurator
         */
        public AggregateAPI parent() {
            return AggregateAPI.this;
        }
        
        /**
         * Defines a key as the application of the given function to this sub-configurator's intermediate result.
         *
         * @param mapper a function to be applied to this sub-configurator's intermediate result
         * @return this configurator
         */
        public Keys<T> key(Function<? super T, ?> mapper) {
            Objects.requireNonNull(mapper);
            return keyHelper(true, Utils.tempField(), mapper);
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
     * @see AggregateAPI#aggs(Collector, Consumer)
     * @param <T> the intermediate result type
     */
    public class Aggs<T> extends CollectorBox {
        final Collector<? super Record, ?, ? extends T> collector;
        final List<AggObjectMapper> children = new ArrayList<>();
        
        Aggs(Collector<? super Record, ?, ? extends T> collector) {
            this.collector = collector;
        }
        
        /**
         * Returns the parent of this sub-configurator.
         *
         * @return the parent of this sub-configurator
         */
        public AggregateAPI parent() {
            return AggregateAPI.this;
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
    
    /**
     * A sub-configurator used to define fields of an aggregate operation that aggregate over values emitted to them by
     * a shared routing function.
     *
     * @see AggregateAPI#route(BiConsumer, Consumer)
     * @param <T> the type of values emitted by the routing function
     */
    public class Route<T> extends CollectorBox {
        final BiConsumer<? super Record, ? super BiConsumer<Field<?>, T>> router;
        final List<RoutedObjectCollector> children = new ArrayList<>();
        
        Route(BiConsumer<? super Record, ? super BiConsumer<Field<?>, T>> router) {
            this.router = router;
        }
    
        /**
         * Returns the parent of this sub-configurator.
         *
         * @return the parent of this sub-configurator
         */
        public AggregateAPI parent() {
            return AggregateAPI.this;
        }
    
        /**
         * Defines (or redefines) the given field as an aggregate over values emitted to the field by this
         * sub-configurator's routing function.
         *
         * @param field the field
         * @param collector the collector that describes the aggregate operation over input values
         * @return this configurator
         * @param <U> the value type of the field
         */
        public <U> Route<T> aggField(Field<U> field, Collector<? super T, ?, ? extends U> collector) {
            Objects.requireNonNull(field);
            Objects.requireNonNull(collector);
            int index = indexByField.computeIfAbsent(field, k -> definitions.size());
            RoutedObjectCollector def = new RoutedObjectCollector(field, collector);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        // TODO: Optimize by reusing the outer collector's array from the start,
        //  instead of flattening this collector's array into it at the end.
        
        @Override
        Collector<? super Record, ?, ?> collector() {
            int size = children.size();
            Supplier<?>[] suppliers = new Supplier[size];
            BiConsumer<?, ?>[] accumulators = new BiConsumer[size];
            BinaryOperator<?>[] combiners = new BinaryOperator[size];
            Function<?, ?>[] finishers = new Function[size];
            Map<Field<?>, Integer> indexByField = new HashMap<>(size);
    
            for (int i = 0; i < size; i++) {
                Collector<?, ?, ?> collector = children.get(i).collector;
                suppliers[i] = collector.supplier();
                accumulators[i] = collector.accumulator();
                combiners[i] = collector.combiner();
                finishers[i] = collector.finisher();
                indexByField.put(children.get(i).field, i);
            }
            
            return Collector.of(
                () -> {
                    Object[] arr = new Object[size+1];
                    for (int i = 0; i < size; i++)
                        arr[i] = suppliers[i].get();
                    // Attach sink to collector state to avoid repetitive creation in the accumulator
                    BiConsumer<Field<?>, T> sink = (key, value) -> {
                        Integer i = indexByField.get(key);
                        if (i != null)
                            accumulators[i].accept(Utils.cast(arr[i]), Utils.cast(value));
                    };
                    arr[size] = sink;
                    return arr;
                },
                (a, t) -> router.accept(t, Utils.cast(a[size])),
                (a, b) -> {
                    for (int i = 0; i < size; i++)
                        a[i] = combiners[i].apply(Utils.cast(a[i]), Utils.cast(b[i]));
                    return a;
                },
                a -> {
                    // Result includes the sink in the last position, but we just ignore it when populating end-array
                    // values in the accept() override.
                    for (int i = 0; i < size; i++)
                        a[i] = finishers[i].apply(Utils.cast(a[i]));
                    return a;
                }
            );
        }
        
        @Override
        void accept(Object obj, Object[] arr) {
            Object[] childValues = (Object[]) obj;
            for (int i = 0; i < children.size(); i++)
                children.get(i).accept(childValues[i], arr);
        }
        
        private class RoutedObjectCollector {
            final Field<?> field;
            final Collector<? super T, ?, ?> collector;
            int localIndex;
        
            RoutedObjectCollector(Field<?> field, Collector<? super T, ?, ?> collector) {
                this.field = field;
                this.collector = collector;
            }
        
            void accept(Object obj, Object[] arr) {
                arr[localIndex] = obj;
            }
            
            Route<T> addToParent() {
                boolean isParentUnseen = Route.this.children.isEmpty();
                Route.this.children.add(this);
                return isParentUnseen ? Route.this : null;
            }
        }
    }
}
