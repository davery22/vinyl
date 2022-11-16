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

import vinyl.Record.LinkedRecord;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A configurator used to define a select operation on a {@link RecordStream record-stream}. A select operation maps
 * input records to output records.
 *
 * <p>Fields defined on the configurator (or any sub-configurators) become fields on a resultant {@link Header header}.
 * The header fields are in order of definition on the configurator(s). A field may be redefined on the configurator(s),
 * in which case its definition is replaced, but its original position in the header is retained.
 *
 * @see RecordStream#select
 */
public class SelectAPI {
    private final RecordStream stream;
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<Object> definitions = new ArrayList<>();
    
    SelectAPI(RecordStream stream) {
        this.stream = stream;
    }
    
    /**
     * Defines (or redefines) each field from the input stream as the lookup of the same field on each input record.
     *
     * @return this configurator
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SelectAPI allFields() {
        Field<?>[] fields = stream.header.fields;
        for (int i = 0; i < fields.length; i++) {
            FieldPin pin = new FieldPin(fields[i], i);
            field((Field) fields[i], record -> record.get(pin));
        }
        return this;
    }
    
    /**
     * Defines (or redefines) each field from the input stream, excluding the given excluded fields, as the lookup of
     * the same field on each input record.
     *
     * @param excluded the fields to be excluded
     * @return this configurator
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SelectAPI allFieldsExcept(Field<?>... excluded) {
        Set<Field<?>> excludedSet = Set.of(excluded);
        Field<?>[] fields = stream.header.fields;
        for (int i = 0; i < fields.length; i++)
            if (!excludedSet.contains(fields[i])) {
                FieldPin pin = new FieldPin(fields[i], i);
                field((Field) fields[i], record -> record.get(pin));
            }
        return this;
    }
    
    /**
     * Defines (or redefines) the given field as the lookup of the same field on each input record.
     *
     * @param field the field
     * @return this configurator
     * @throws NoSuchElementException if the stream header does not contain the given field
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SelectAPI field(Field<?> field) {
        FieldPin pin = stream.header.pin(field);
        return field((Field) field, record -> record.get(pin));
    }
    
    /**
     * Defines (or redefines) the given field as the application of the given function to each input record.
     *
     * @param field the field
     * @param mapper a function to apply to each input record
     * @return this configurator
     * @param <T> the value type of the field
     */
    public <T> SelectAPI field(Field<T> field, Function<? super Record, ? extends T> mapper) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(mapper);
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        RecordMapper def = new RecordMapper(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    /**
     * Defines (or redefines) each of the given fields as the lookup of the same field on each input record.
     *
     * @param fields the fields
     * @return this configurator
     * @throws NoSuchElementException if the stream header does not contain a given field
     */
    public SelectAPI fields(Field<?>... fields) {
        for (Field<?> field : fields)
            field(field);
        return this;
    }
    
    /**
     * Configures a sub-configurator, that may define (or redefine) fields in terms of the result of applying the given
     * function to each input record. If no fields are defined by the sub-configurator, possibly due to later field
     * redefinitions, the entire sub-configurator is discarded.
     *
     * @param mapper a function to apply to each input record
     * @param config a consumer of the sub-configurator
     * @return this configurator
     * @param <T> the result type of the function
     */
    public <T> SelectAPI fields(Function<? super Record, ? extends T> mapper, Consumer<Fields<T>> config) {
        Objects.requireNonNull(mapper);
        config.accept(new Fields<>(mapper));
        return this;
    }
    
    /**
     * Configures a sub-configurator, that may define (or redefine) fields as analytic functions over partitions of the
     * input stream. If no fields are defined by the sub-configurator, possibly due to later field redefinitions, the
     * entire sub-configurator is discarded.
     *
     * @param config a consumer of the sub-configurator
     * @return this configurator
     */
    public SelectAPI window(Consumer<Window> config) {
        config.accept(new Window());
        return this;
    }
    
    RecordStream accept(Consumer<SelectAPI> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        // Final mappers will combine ObjectMapper children into their parent.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(indexByField);
        List<Mapper> finalMappers = new ArrayList<>();
        List<Window> finalWindows = new ArrayList<>();
        
        for (Object definition : definitions) {
            if (definition instanceof RecordMapper)
                finalMappers.add((RecordMapper) definition);
            else if (definition instanceof Fields.ObjectMapper) {
                Fields<?>.ObjectMapper def = (Fields<?>.ObjectMapper) definition;
                Fields<?> parent = def.addToParent();
                if (parent != null)
                    finalMappers.add(parent);
            } else if (definition instanceof Window.FieldRecordMapper) {
                Window.FieldRecordMapper<?> def = (Window.FieldRecordMapper<?>) definition;
                Window parent = def.addToParent();
                if (parent != null)
                    finalWindows.add(parent);
            } else {
                assert definition instanceof Window.Fields.FieldObjectMapper;
                Window.Fields<?>.FieldObjectMapper<?> def = (Window.Fields<?>.FieldObjectMapper<?>) definition;
                Window parent = def.addToParent();
                if (parent != null)
                    finalWindows.add(parent);
            }
        }
        
        // Prep the stream transformation.
        int size = definitions.size();
        Header nextHeader = new Header(finalIndexByField);
        Stream<Record> nextStream;
        
        if (finalWindows.isEmpty())
            nextStream = stream.stream.map(record -> {
                Object[] arr = new Object[size];
                for (Mapper mapper : finalMappers)
                    mapper.accept(record, arr);
                return new Record(nextHeader, arr);
            });
        else {
            nextStream = streamWindows(stream.stream.map(record -> new LinkedRecord(record, new Object[size])),
                                       finalWindows)
                .map(record -> {
                    Object[] arr = record.out;
                    for (Mapper mapper : finalMappers)
                        mapper.accept(record, arr);
                    return new Record(nextHeader, arr);
                });
        }
        
        return new RecordStream(nextHeader, nextStream);
    }
    
    private Stream<LinkedRecord> streamWindows(Stream<LinkedRecord> stream, List<Window> windows) {
        List<Collector<LinkedRecord, ?, Map<List<?>, List<LinkedRecord>>>> collectors = new ArrayList<>(windows.size());
        for (Window window : windows)
            // Optimization: skip grouping step for keyless (non-partitioned) windows.
            if (window.keyCount == 0)
                collectors.add(Collectors.collectingAndThen(Collectors.toList(),
                                                            list -> list.isEmpty()
                                                                ? Collections.emptyMap()
                                                                : Collections.singletonMap(Collections.emptyList(), list)));
            else {
                // Copying keyMapperChildren to array also acts as a defensive copy against bad-actor user code.
                // There is no need to do the same for windowFnChildren (used in Window::accept), as they can only be added
                // by trusted code.
                Function<Record, List<?>> classifier = classifierFor(window.keyMapperChildren.toArray(new Mapper[0]), window.keyCount);
                collectors.add(Collectors.groupingBy(classifier, Collectors.toList()));
            }
        Function<Collection<List<LinkedRecord>>, Stream<List<LinkedRecord>>> toStream =
            stream.isParallel() ? Collection::parallelStream : Collection::stream;
        return StreamSupport.stream(
            () -> {
                // Process windows sequentially to minimize memory usage.
                Stream<LinkedRecord> curr = stream;
                for (int i = 0; ; i++) {
                    Collection<List<LinkedRecord>> partitions = curr.collect(collectors.get(i)).values();
                    Stream<List<LinkedRecord>> next = toStream.apply(partitions).peek(windows.get(i)::accept);
                    if (i == windows.size()-1) // Optimization: Return the values stream spliterator, so that we can report Spliterator.SIZED
                        return next.spliterator();
                    curr = next.flatMap(Collection::stream);
                }
            },
            Spliterator.SIZED | Spliterator.NONNULL | Spliterator.IMMUTABLE,
            stream.isParallel()
        ).flatMap(Collection::stream)
            .onClose(stream::close);
    }
    
    private Function<Record, List<?>> classifierFor(Mapper[] mappers, int keysSize) {
        return record -> {
            Object[] arr = new Object[keysSize];
            for (Mapper mapper : mappers)
                mapper.accept(record, arr);
            return Arrays.asList(arr); // convert to List for equals/hashCode
        };
    }
    
    private abstract static class Mapper {
        abstract void accept(Record record, Object[] arr);
    }
    
    private abstract static class WindowFunction {
        abstract void accept(List<LinkedRecord> records);
    }
    
    private static class RecordMapper extends Mapper {
        final int index;
        final Function<? super Record, ?> mapper;
        
        RecordMapper(int index, Function<? super Record, ?> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        @Override
        void accept(Record record, Object[] arr) {
            arr[index] = mapper.apply(record);
        }
    }
    
    /**
     * A sub-configurator used to define fields of a select operation that depend on a common intermediate result.
     *
     * @see SelectAPI#fields(Function, Consumer)
     * @param <T> the intermediate result type
     */
    public class Fields<T> extends Mapper {
        final Function<? super Record, ? extends T> mapper;
        final List<ObjectMapper> children = new ArrayList<>();
        
        Fields(Function<? super Record, ? extends T> mapper) {
            this.mapper = mapper;
        }
    
        /**
         * Defines (or redefines) the given field as the application of the given function to this sub-configurator's
         * intermediate result.
         *
         * @param field the field
         * @param mapper a function to apply to this sub-configurator's intermediate result
         * @return this configurator
         * @param <U> the value type of the field
         */
        public <U> Fields<T> field(Field<U> field, Function<? super T, ? extends U> mapper) {
            Objects.requireNonNull(field);
            Objects.requireNonNull(mapper);
            int index = indexByField.computeIfAbsent(field, k -> definitions.size());
            ObjectMapper def = new ObjectMapper(index, mapper);
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
    
        private class ObjectMapper {
            final int index;
            final Function<? super T, ?> mapper;
            
            ObjectMapper(int index, Function<? super T, ?> mapper) {
                this.index = index;
                this.mapper = mapper;
            }
            
            void accept(T in, Object[] arr) {
                arr[index] = mapper.apply(in);
            }
            
            Fields<T> addToParent() {
                boolean isParentUnseen = Fields.this.children.isEmpty();
                Fields.this.children.add(this);
                return isParentUnseen ? Fields.this : null;
            }
        }
    }
    
    /**
     * A sub-configurator used to define fields of a select operation in terms of analytic functions over partitions of
     * the input stream.
     *
     * <p>A window may define zero or more keys, that together group the input stream into non-overlapping partitions.
     * Each partition will capture one or more input records, each of which corresponds to exactly one output record.
     *
     * <p>A window may define fields as analytic functions. An analytic function is applied per-partition, and emits one
     * value per input record in the partition, which becomes the value of the field on the corresponding output record
     * from the partition. Alternatively, an analytic function may emit just one value for the partition, which becomes
     * the value of the field on all output records from the partition.
     *
     * @see SelectAPI#window(Consumer)
     */
    public class Window {
        final List<Mapper> keyMapperChildren = new ArrayList<>();
        final List<WindowFunction> windowFnChildren = new ArrayList<>();
        int keyCount = 0;
        
        Window() {} // Prevent default public constructor
    
        /**
         * Defines a key as the lookup of the given field on each input record.
         *
         * @param field the field
         * @return this configurator
         * @throws NoSuchElementException if the stream header does not contain the given field
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Window key(Field<?> field) {
            FieldPin pin = stream.header.pin(field);
            return key(record -> record.get(pin));
        }
    
        /**
         * Defines a key as the application of the given function to each input record.
         *
         * @param mapper a function to be applied to each input record
         * @return this configurator
         */
        public Window key(Function<? super Record, ?> mapper) {
            // Since all keys are temps and cannot be overridden, can bypass definitions and indexByField,
            // and just add to parent right away. This simplifies much.
            Objects.requireNonNull(mapper);
            new KeyRecordMapper(keyCount++, mapper).addToParent();
            return this;
        }
    
        /**
         * Shorthand for calling {@link #field(Field, Comparator, BiConsumer)}, passing a {@code null} comparator.
         *
         * @param field the field
         * @param mapper an analytic function to be applied to input records in each partition
         * @return this configurator
         * @param <T> the value type of the field
         */
        public <T> Window field(Field<T> field,
                                BiConsumer<? super List<Record>, ? super Consumer<T>> mapper) {
            return field(field, null, mapper);
        }
    
        /**
         * Defines (or redefines) the given field in terms of the given analytic function, applied to input records in
         * each partition.
         *
         * <p>The function is called with a non-empty, unmodifiable list of input records and a consumer of field
         * values. The function must call the consumer exactly once, or once per record in the partition; otherwise an
         * {@link IllegalStateException} will be thrown during evaluation. If the consumer is called once, the value
         * passed to it will be assigned to the field on all output records from the partition. If the consumer is
         * called multiple times, each value passed to it will be assigned to the field on the next output record, in
         * ordered correspondence with the input records (as ordered by the given comparator, if any).
         *
         * <p>A comparator may be provided to sort input records in the partition prior to applying the analytic
         * function. Some analytic functions may depend on this for proper ordered correspondence. If the comparator is
         * {@code null}, input records are left unsorted.
         *
         * @param field the field
         * @param comparator a comparator used to sort the input records in the partition before applying the function.
         *                   May be {@code null}, in which case input records are left unsorted.
         * @param mapper an analytic function to be applied to input records in each partition
         * @return this configurator
         * @param <T> the value type of the field
         */
        public <T> Window field(Field<T> field,
                                Comparator<? super Record> comparator,
                                BiConsumer<? super List<Record>, ? super Consumer<T>> mapper) {
            Objects.requireNonNull(field);
            Objects.requireNonNull(mapper);
            int index = indexByField.computeIfAbsent(field, k -> definitions.size());
            FieldRecordMapper<T> def = new FieldRecordMapper<>(field, index, comparator, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
    
        /**
         * Defines keys from each of the given fields, as the lookup of the same field on each input record.
         *
         * @param fields the fields
         * @return this configurator
         * @throws NoSuchElementException if the stream header does not contain a given field
         */
        public Window keys(Field<?>... fields) {
            for (Field<?> field : fields)
                key(field);
            return this;
        }
    
        /**
         * Configures a sub-configurator, that may define keys in terms of the result of applying the given function to
         * each input record. If no keys are defined by the sub-configurator, the entire sub-configurator is discarded.
         *
         * @param mapper a function to be applied to each input record
         * @param config a consumer of the sub-configurator
         * @return this configurator
         * @param <T> the result type of the function
         */
        public <T> Window keys(Function<? super Record, ? extends T> mapper,
                               Consumer<Window.Keys<T>> config) {
            Objects.requireNonNull(mapper);
            config.accept(new Window.Keys<>(mapper));
            return this;
        }
    
        /**
         * Shorthand for calling {@link #fields(Comparator, Function, Consumer)}, passing a {@code null} comparator.
         *
         * @param mapper a function to be applied to input records in each partition
         * @param config a consumer of the sub-configurator
         * @return this configurator
         * @param <T> the result type of the function
         */
        public <T> Window fields(Function<? super List<Record>, ? extends T> mapper,
                                 Consumer<Fields<T>> config) {
            return fields(null, mapper, config);
        }
    
        /**
         * Shorthand for calling {@link #fields(Comparator, Function, Consumer)}, passing an {@link Function#identity()
         * identity} mapper.
         *
         * @param comparator a comparator used to sort the input records in the partition before applying any enclosed
         *                   analytic functions. May be {@code null}, in which case input records are left unsorted.
         * @param config a consumer of the sub-configurator
         * @return this configurator
         */
        public Window fields(Comparator<? super Record> comparator,
                             Consumer<Fields<List<Record>>> config) {
            return fields(comparator, Function.identity(), config);
        }
    
        /**
         * Configures a sub-configurator, that may define (or redefine) fields in terms of analytic functions that take
         * as input the result of applying the given function to input records in each partition. The function is called
         * with a non-empty, unmodifiable list of input records. Enclosed analytic functions follow the same "once or
         * once-per-record" emission rules as usual, even if size information is not preserved by the given function's
         * result.
         *
         * <p>A comparator may be provided to sort input records in the partition prior to applying the given function.
         * Some enclosed analytic functions may depend on this for proper ordered correspondence. If the comparator is
         * {@code null}, input records are left unsorted.
         *
         * <p>If no fields are defined by the sub-configurator, possibly due to later field redefinitions, the entire
         * sub-configurator is discarded.
         *
         * @param comparator a comparator used to sort the input records in the partition before applying the function,
         *                   or any enclosed analytic functions. May be {@code null}, in which case input records are
         *                   left unsorted.
         * @param mapper a function to be applied to input records in each partition
         * @param config a consumer of the sub-configurator
         * @return this configurator
         * @param <T> the result type of the function
         * @see #field(Field, Comparator, BiConsumer)
         */
        public <T> Window fields(Comparator<? super Record> comparator,
                                 Function<? super List<Record>, ? extends T> mapper,
                                 Consumer<Fields<T>> config) {
            Objects.requireNonNull(mapper);
            config.accept(new Fields<>(comparator, mapper));
            return this;
        }
        
        void accept(List<LinkedRecord> records) {
            // It would be nice if we preserved encounter order for unordered window functions,
            // but that would require processing all windows in the same pass instead of feeding them into each other,
            // and that would be much more memory intensive.
            windowFnChildren.forEach(child -> child.accept(records));
        }
        
        private class KeyRecordMapper extends Mapper {
            final Function<? super Record, ?> mapper;
            final int localIndex;
            
            KeyRecordMapper(int localIndex, Function<? super Record, ?> mapper) {
                this.localIndex = localIndex;
                this.mapper = mapper;
            }
    
            @Override
            void accept(Record record, Object[] arr) {
                arr[localIndex] = mapper.apply(record);
            }
            
            void addToParent() {
                Window.this.keyMapperChildren.add(this);
            }
        }
        
        private class FieldRecordMapper<T> extends WindowFunction {
            final Field<T> field;
            final int index;
            final Comparator<? super Record> comparator;
            final BiConsumer<? super List<Record>, ? super Consumer<T>> mapper;
            
            FieldRecordMapper(Field<T> field,
                              int index,
                              Comparator<? super Record> comparator,
                              BiConsumer<? super List<Record>, ? super Consumer<T>> mapper) {
                this.field = field;
                this.index = index;
                this.comparator = comparator;
                this.mapper = mapper;
            }
    
            @Override
            void accept(List<LinkedRecord> records) {
                if (comparator != null)
                    records.sort(comparator);
                WindowSink<T> sink = new WindowSink<>(field, index, records);
                mapper.accept(Collections.unmodifiableList(records), sink);
                sink.finish();
            }
            
            Window addToParent() {
                boolean isParentUnseen = Window.this.windowFnChildren.isEmpty();
                Window.this.windowFnChildren.add(this);
                return isParentUnseen ? Window.this : null;
            }
        }
    
        /**
         * A sub-configurator used to define keys of a window that depend on a common intermediate result.
         *
         * @see Window#keys(Function, Consumer)
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
             * @param mapper a function to be applied to this sub-configurator's intermediate result.
             * @return this configurator
             */
            public Keys<T> key(Function<? super T, ?> mapper) {
                // Since all keys are temps and cannot be overridden, can bypass definitions and indexByField,
                // and just add to parent right away. This simplifies much.
                Objects.requireNonNull(mapper);
                new KeyObjectMapper(keyCount++, mapper).addToParent();
                return this;
            }
            
            @Override
            void accept(Record record, Object[] arr) {
                T obj = mapper.apply(record);
                children.forEach(child -> child.accept(obj, arr));
            }
            
            void addToParent() {
                Window.this.keyMapperChildren.add(this);
            }
            
            private class KeyObjectMapper {
                final Function<? super T, ?> mapper;
                final int localIndex;
                
                KeyObjectMapper(int localIndex, Function<? super T, ?> mapper) {
                    this.localIndex = localIndex;
                    this.mapper = mapper;
                }
                
                void accept(T obj, Object[] arr) {
                    arr[localIndex] = mapper.apply(obj);
                }
                
                void addToParent() {
                    boolean isParentUnseen = Keys.this.children.isEmpty();
                    Keys.this.children.add(this);
                    if (isParentUnseen)
                        Keys.this.addToParent();
                }
            }
        }
    
        /**
         * A sub-configurator used to define fields of a window that depend on a common partition ordering and/or a
         * common intermediate result.
         *
         * @see Window#fields(Comparator, Function, Consumer)
         * @param <T> the intermediate result type
         */
        public class Fields<T> extends WindowFunction {
            final Comparator<? super Record> comparator;
            final Function<? super List<Record>, ? extends T> mapper;
            final List<FieldObjectMapper<?>> children = new ArrayList<>();
            
            Fields(Comparator<? super Record> comparator,
                   Function<? super List<Record>, ? extends T> mapper) {
                this.comparator = comparator;
                this.mapper = mapper;
            }
    
            /**
             * Defines (or redefines) the given field in terms of the given analytic function, applied to this
             * sub-configurator's intermediate result in each partition.
             *
             * <p>The function is called with the intermediate result and a consumer of field values. The function must
             * call the consumer exactly once, or once per record in the partition (even if size information was not
             * preserved by the intermediate result); otherwise an {@link IllegalStateException} will be thrown during
             * evaluation. If the consumer is called once, the value passed to it will be assigned to the field on all
             * output records from the partition. If the consumer is called multiple times, each value passed to it will
             * be assigned to the field on the next output record, in ordered correspondence with the input records (as
             * ordered by this sub-configurator's comparator, if any).
             *
             * @param field the field
             * @param mapper an analytic function to be applied to this sub-configurator's intermediate result in each
             *               partition
             * @return this configurator
             * @param <U> the value type of the field
             */
            public <U> Fields<T> field(Field<U> field,
                                       BiConsumer<? super T, ? super Consumer<U>> mapper) {
                Objects.requireNonNull(field);
                Objects.requireNonNull(mapper);
                int index = indexByField.computeIfAbsent(field, k -> definitions.size());
                FieldObjectMapper<U> def = new FieldObjectMapper<>(field, index, mapper);
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
                return this;
            }
    
            @Override
            void accept(List<LinkedRecord> records) {
                if (comparator != null)
                    records.sort(comparator);
                T obj = mapper.apply(Collections.unmodifiableList(records));
                children.forEach(child -> child.accept(records, obj));
            }
            
            Window addToParent() {
                boolean isParentUnseen = Window.this.windowFnChildren.isEmpty();
                Window.this.windowFnChildren.add(this);
                return isParentUnseen ? Window.this : null;
            }
            
            private class FieldObjectMapper<U> {
                final Field<U> field;
                final int index;
                final BiConsumer<? super T, ? super Consumer<U>> mapper;
                
                FieldObjectMapper(Field<U> field,
                                  int index,
                                  BiConsumer<? super T, ? super Consumer<U>> mapper) {
                    this.field = field;
                    this.index = index;
                    this.mapper = mapper;
                }
                
                void accept(List<LinkedRecord> records, T obj) {
                    WindowSink<U> sink = new WindowSink<>(field, index, records);
                    mapper.accept(obj, sink);
                    sink.finish();
                }
                
                Window addToParent() {
                    boolean isParentUnseen = Fields.this.children.isEmpty();
                    Fields.this.children.add(this);
                    return isParentUnseen ? Fields.this.addToParent() : null;
                }
            }
        }
    }
    
    private static String badWindowMessage(Field<?> field, int emitted, int records) {
        return String.format("Analytic function for field [%s] must emit exactly one value, or one value per record; " +
                                 "emitted %d values for %d records", field, emitted, records);
    }
    
    private static class WindowSink<T> implements Consumer<T> {
        final Field<?> field;
        final int index;
        final List<LinkedRecord> records;
        int i = 0;
        
        WindowSink(Field<?> field, int index, List<LinkedRecord> records) {
            this.field = field;
            this.index = index;
            this.records = records;
        }
        
        @Override
        public void accept(T it) {
            try {
                records.get(i++).out[index] = it;
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalStateException(badWindowMessage(field, i-1, records.size()));
            }
        }
        
        void finish() {
            if (i == 1) {
                Object first = records.get(0).out[index];
                records.forEach(record -> record.out[index] = first);
            } else if (i != records.size()) {
                throw new IllegalStateException(badWindowMessage(field, i, records.size()));
            }
        }
    }
}
