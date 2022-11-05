package vinyl;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SelectAPI {
    private final RecordStream stream;
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<Object> definitions = new ArrayList<>();
    
    SelectAPI(RecordStream stream) {
        this.stream = stream;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SelectAPI allFields() {
        Field<?>[] fields = stream.header.fields;
        for (int i = 0; i < fields.length; i++) {
            FieldPin pin = new FieldPin(fields[i], i);
            field((Field) fields[i], record -> record.get(pin));
        }
        return this;
    }
    
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
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SelectAPI field(Field<?> field) {
        FieldPin pin = stream.header.pin(field);
        return field((Field) field, record -> record.get(pin));
    }
    
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
    
    public SelectAPI fields(Field<?>... fields) {
        for (Field<?> field : fields)
            field(field);
        return this;
    }
    
    public <T> SelectAPI fields(Function<? super Record, ? extends T> mapper, Consumer<Fields<T>> config) {
        Objects.requireNonNull(mapper);
        config.accept(new Fields<>(mapper));
        return this;
    }
    
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
                    Object[] arr = record.next;
                    for (Mapper mapper : finalMappers)
                        mapper.accept(record, arr);
                    return new Record(nextHeader, arr);
                });
        }
        
        return new RecordStream(nextHeader, nextStream);
    }
    
    private Stream<LinkedRecord> streamWindows(Stream<LinkedRecord> stream, List<Window> windows) {
        Stream<LinkedRecord> curr = stream;
        // Process windows sequentially to minimize memory usage.
        // TODO: Merge keyless (non-partitioned) windows, and skip groupingBy step for those.
        for (Window window : windows) {
            // Copying keyMapperChildren to array also acts as a defensive copy against bad-actor user code.
            // There is no need to do the same for windowFnChildren (used in Window::accept), as they can only be added
            // by trusted code.
            Function<Record, List<?>> classifier = classifierFor(window.keyMapperChildren.toArray(new Mapper[0]), window.keyCount);
            Collector<LinkedRecord, ?, Map<List<?>, List<LinkedRecord>>> collector = Collectors.groupingBy(classifier, Collectors.toList());
            Stream<LinkedRecord> prev = curr;
            curr = StreamSupport.stream(
                () -> {
                    Collection<List<LinkedRecord>> partitions = prev.collect(collector).values();
                    (stream.isParallel() ? partitions.parallelStream() : partitions.stream())
                        .forEach(window::accept);
                    return partitions.spliterator();
                },
                Spliterator.SIZED,
                stream.isParallel()
            ).flatMap(Collection::stream);
        }
        return curr.onClose(stream::close);
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
    
    public class Fields<T> extends Mapper {
        final Function<? super Record, ? extends T> mapper;
        final List<ObjectMapper> children = new ArrayList<>();
        
        Fields(Function<? super Record, ? extends T> mapper) {
            this.mapper = mapper;
        }
        
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
    
    public class Window {
        final List<Mapper> keyMapperChildren = new ArrayList<>();
        final List<WindowFunction> windowFnChildren = new ArrayList<>();
        int keyCount = 0;
        
        Window() {} // Prevent default public constructor
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Window key(Field<?> field) {
            FieldPin pin = stream.header.pin(field);
            return key(record -> record.get(pin));
        }
    
        public Window key(Function<? super Record, ?> mapper) {
            // Since all keys are temps and cannot be overridden, can bypass definitions and indexByField,
            // and just add to parent right away. This simplifies much.
            Objects.requireNonNull(mapper);
            new KeyRecordMapper(keyCount++, mapper).addToParent();
            return this;
        }
        
        public <T> Window field(Field<T> field,
                                BiConsumer<? super List<Record>, ? super Consumer<T>> mapper) {
            return field(field, null, mapper);
        }
    
        public <T> Window field(Field<T> field,
                                Comparator<? super Record> comparator,
                                BiConsumer<? super List<Record>, ? super Consumer<T>> mapper) {
            Objects.requireNonNull(field);
            Objects.requireNonNull(mapper);
            int index = indexByField.computeIfAbsent(field, k -> definitions.size());
            FieldRecordMapper<T> def = new FieldRecordMapper<>(index, comparator, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
    
        public Window keys(Field<?>... fields) {
            for (Field<?> field : fields)
                key(field);
            return this;
        }
        
        public <T> Window keys(Function<? super Record, ? extends T> mapper,
                               Consumer<Window.Keys<T>> config) {
            Objects.requireNonNull(mapper);
            config.accept(new Window.Keys<>(mapper));
            return this;
        }
        
        public <T> Window fields(Function<? super List<Record>, ? extends T> mapper,
                                 Consumer<Fields<T>> config) {
            Objects.requireNonNull(mapper);
            config.accept(new Fields<>(null, mapper));
            return this;
        }
    
        public <T> Window fields(Comparator<? super Record> comparator,
                                 Consumer<Fields<List<Record>>> config) {
            config.accept(new Fields<>(comparator, Function.identity()));
            return this;
        }
    
        public <T> Window fields(Comparator<? super Record> comparator,
                                 Function<? super List<Record>, ? extends T> mapper,
                                 Consumer<Fields<T>> config) {
            Objects.requireNonNull(mapper);
            config.accept(new Fields<>(comparator, mapper));
            return this;
        }
        
        void accept(List<LinkedRecord> records) {
            // TODO: Process unordered window functions first? (to preserve original encounter order)
            //  But encounter order would still be undefined so long as we feed window results into each other.
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
            final int index;
            final Comparator<? super Record> comparator;
            final BiConsumer<? super List<Record>, ? super Consumer<T>> mapper;
            
            FieldRecordMapper(int index,
                              Comparator<? super Record> comparator,
                              BiConsumer<? super List<Record>, ? super Consumer<T>> mapper) {
                this.index = index;
                this.comparator = comparator;
                this.mapper = mapper;
            }
    
            @Override
            void accept(List<LinkedRecord> records) {
                if (comparator != null)
                    records.sort(comparator);
                class Sink implements Consumer<T> {
                    int i = 0;
                    public void accept(T it) {
                        // Throws IOOBE
                        records.get(i++).next[index] = it;
                    }
                    void finish() {
                        if (i == 1) {
                            Object first = records.get(0).next[index];
                            records.forEach(record -> record.next[index] = first);
                        } else if (i != records.size()) {
                            throw new IllegalStateException("Window function must produce exactly one value, or one value per record");
                        }
                    }
                }
                Sink sink = new Sink();
                mapper.accept(Collections.unmodifiableList(records), sink);
                sink.finish();
            }
            
            Window addToParent() {
                boolean isParentUnseen = Window.this.windowFnChildren.isEmpty();
                Window.this.windowFnChildren.add(this);
                return isParentUnseen ? Window.this : null;
            }
        }
    
        public class Keys<T> extends Mapper {
            final Function<? super Record, ? extends T> mapper;
            final List<KeyObjectMapper> children = new ArrayList<>();
            
            Keys(Function<? super Record, ? extends T> mapper) {
                this.mapper = mapper;
            }
            
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
        
        public class Fields<T> extends WindowFunction {
            final Comparator<? super Record> comparator;
            final Function<? super List<Record>, ? extends T> mapper;
            final List<FieldObjectMapper<?>> children = new ArrayList<>();
            
            Fields(Comparator<? super Record> comparator,
                   Function<? super List<Record>, ? extends T> mapper) {
                this.comparator = comparator;
                this.mapper = mapper;
            }
            
            public <U> Fields<T> field(Field<U> field,
                                       BiConsumer<? super T, ? super Consumer<U>> mapper) {
                Objects.requireNonNull(field);
                Objects.requireNonNull(mapper);
                int index = indexByField.computeIfAbsent(field, k -> definitions.size());
                FieldObjectMapper<U> def = new FieldObjectMapper<>(index, mapper);
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
                final int index;
                final BiConsumer<? super T, ? super Consumer<U>> mapper;
                
                FieldObjectMapper(int index, BiConsumer<? super T, ? super Consumer<U>> mapper) {
                    this.index = index;
                    this.mapper = mapper;
                }
                
                void accept(List<LinkedRecord> records, T obj) {
                    class Sink implements Consumer<U> {
                        int i = 0;
                        public void accept(U it) {
                            // Throws IOOBE
                            records.get(i++).next[index] = it;
                        }
                        void finish() {
                            if (i == 1) {
                                Object first = records.get(0).next[index];
                                records.forEach(record -> record.next[index] = first);
                            } else if (i != records.size()) {
                                throw new IllegalStateException("Window function must produce exactly one value, or one value per record");
                            }
                        }
                    }
                    Sink sink = new Sink();
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
    
    // Used by window / analytical functions.
    static class LinkedRecord extends Record {
        final Object[] next;
        
        LinkedRecord(Record curr, Object[] next) {
            super(curr.header, curr.values);
            this.next = next;
        }
    }
}
