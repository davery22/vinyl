package da.tasets;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SelectAPI {
    private final DatasetStream stream;
    private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
    private final List<Object> definitions = new ArrayList<>();
    
    SelectAPI(DatasetStream stream) {
        this.stream = stream;
    }
    
    public SelectAPI all() {
        for (Column<?> column : stream.header.columns)
            col(column);
        return this;
    }
    
    public SelectAPI allExcept(Column<?>... excluded) {
        Set<Column<?>> excludedSet = Set.of(excluded);
        for (Column<?> column : stream.header.columns)
            if (!excludedSet.contains(column))
                col(column);
        return this;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SelectAPI col(Column<?> column) {
        Locator locator = new Locator(column, stream.header.indexOf(column));
        return col((Column) column, row -> row.get(locator));
    }
    
    public <T> SelectAPI col(Column<T> column, Function<? super Row, ? extends T> mapper) {
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        RowMapper def = new RowMapper(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    public SelectAPI cols(Column<?>... columns) {
        for (Column<?> column : columns)
            col(column);
        return this;
    }
    
    public <T> SelectAPI cols(Function<? super Row, ? extends T> mapper, Consumer<Cols<T>> config) {
        config.accept(new Cols<>(mapper));
        return this;
    }
    
    public SelectAPI window(Consumer<Window> config) {
        config.accept(new Window());
        return this;
    }
    
    DatasetStream accept(Consumer<SelectAPI> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        // Final mappers will combine ObjMapper children into their parent.
        Map<Column<?>, Integer> finalIndexByColumn = Map.copyOf(indexByColumn);
        List<Mapper> finalMappers = new ArrayList<>();
        List<Window> finalWindows = new ArrayList<>();
        
        for (Object definition : definitions) {
            if (definition instanceof RowMapper)
                finalMappers.add((RowMapper) definition);
            else if (definition instanceof Cols.ObjMapper) {
                Cols<?>.ObjMapper def = (Cols<?>.ObjMapper) definition;
                Cols<?> parent = def.addToParent();
                if (parent != null)
                    finalMappers.add(parent);
            } else if (definition instanceof Window.ColRowMapper) {
                Window.ColRowMapper<?> def = (Window.ColRowMapper<?>) definition;
                Window parent = def.addToParent();
                if (parent != null)
                    finalWindows.add(parent);
            } else {
                assert definition instanceof Window.Cols.ColObjMapper;
                Window.Cols<?>.ColObjMapper<?> def = (Window.Cols<?>.ColObjMapper<?>) definition;
                Window parent = def.addToParent();
                if (parent != null)
                    finalWindows.add(parent);
            }
        }
        
        // Prep the row-by-row transformation.
        int size = definitions.size();
        Header nextHeader = new Header(finalIndexByColumn);
        Stream<Row> nextStream;
        
        if (finalWindows.isEmpty())
            nextStream = stream.stream.map(row -> {
                Object[] arr = new Object[size];
                for (Mapper mapper : finalMappers)
                    mapper.accept(row, arr);
                return new Row(nextHeader, arr);
            });
        else {
            nextStream = streamWindows(stream.stream.map(row -> new LinkedRow(row, new Object[size])),
                                       finalWindows)
                .map(row -> {
                    Object[] arr = row.next;
                    for (Mapper mapper : finalMappers)
                        mapper.accept(row, arr);
                    return new Row(nextHeader, row.next);
                });
        }
        
        return new DatasetStream(nextHeader, nextStream);
    }
    
    private Stream<LinkedRow> streamWindows(Stream<LinkedRow> stream, List<Window> windows) {
        Stream<LinkedRow> curr = stream;
        // Process windows sequentially to minimize memory usage.
        // TODO: Merge keyless (non-partitioned) windows, and skip groupingBy step for those.
        for (Window window : windows) {
            // Copying keyMapperChildren to array also acts as a defensive copy against bad-actor user code.
            // There is no need to do the same for windowFnChildren (used in Window::accept), as they can only be added
            // by trusted code.
            Function<Row, List<?>> classifier = classifierFor(window.keyMapperChildren.toArray(Window.KeyMapper[]::new), window.keyCount);
            Collector<LinkedRow, ?, Map<List<?>, List<LinkedRow>>> collector = Collectors.groupingBy(classifier, Collectors.toList());
            Stream<LinkedRow> prev = curr;
            curr = StreamSupport.stream(
                () -> {
                    Collection<List<LinkedRow>> partitions = prev.collect(collector).values();
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
    
    private Function<Row, List<?>> classifierFor(Window.KeyMapper[] mappers, int keysSize) {
        return row -> {
            Object[] arr = new Object[keysSize];
            for (Window.KeyMapper mapper : mappers)
                mapper.accept(row, arr);
            return Arrays.asList(arr); // convert to List for equals/hashCode
        };
    }
    
    private abstract static class Mapper {
        abstract void accept(Row row, Object[] arr);
    }
    
    private static class RowMapper extends Mapper {
        final int index;
        final Function<? super Row, ?> mapper;
        
        RowMapper(int index, Function<? super Row, ?> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        @Override
        void accept(Row row, Object[] arr) {
            arr[index] = mapper.apply(row);
        }
    }
    
    public class Cols<T> extends Mapper {
        final Function<? super Row, ? extends T> mapper;
        final List<ObjMapper> children = new ArrayList<>();
        
        Cols(Function<? super Row, ? extends T> mapper) {
            this.mapper = mapper;
        }
        
        public <U> Cols<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            ObjMapper def = new ObjMapper(index, mapper);
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
    
        private class ObjMapper {
            final int index;
            final Function<? super T, ?> mapper;
            
            ObjMapper(int index, Function<? super T, ?> mapper) {
                this.index = index;
                this.mapper = mapper;
            }
            
            void accept(T in, Object[] arr) {
                arr[index] = mapper.apply(in);
            }
            
            Cols<T> addToParent() {
                boolean isParentUnseen = Cols.this.children.isEmpty();
                Cols.this.children.add(this);
                return isParentUnseen ? Cols.this : null;
            }
        }
    }
    
    public class Window {
        final List<KeyMapper> keyMapperChildren = new ArrayList<>();
        final List<WindowFunction> windowFnChildren = new ArrayList<>();
        int keyCount = 0;
        
        Window() {} // Prevent default public constructor
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Window key(Column<?> column) {
            Locator locator = new Locator(column, stream.header.indexOf(column));
            return key(locator::get);
        }
    
        public Window key(Function<? super Row, ?> mapper) {
            // Since all keys are temps and cannot be overridden, can bypass definitions and indexByColumn,
            // and just add to parent right away. This simplifies much.
            new KeyRowMapper(keyCount++, mapper).addToParent();
            return this;
        }
        
        public <T> Window col(Column<T> column,
                                 BiConsumer<? super List<Row>, ? super Consumer<T>> mapper) {
            return col(column, null, mapper);
        }
    
        public <T> Window col(Column<T> column,
                                 Comparator<? super Row> comparator,
                                 BiConsumer<? super List<Row>, ? super Consumer<T>> mapper) {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            ColRowMapper<T> def = new ColRowMapper<>(index, comparator, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
    
        public Window keys(Column<?>... columns) {
            for (Column<?> column : columns)
                key(column);
            return this;
        }
        
        public <T> Window keys(Function<? super Row, ? extends T> mapper,
                               Consumer<Window.Keys<T>> config) {
            config.accept(new Window.Keys<>(mapper));
            return this;
        }
        
        public <T> Window cols(Function<? super List<Row>, ? extends T> mapper,
                               Consumer<Window.Cols<T>> config) {
            config.accept(new Window.Cols<>(null, mapper));
            return this;
        }
    
        public <T> Window cols(Comparator<? super Row> comparator,
                               Consumer<Window.Cols<List<Row>>> config) {
            config.accept(new Window.Cols<>(comparator, Function.identity()));
            return this;
        }
    
        public <T> Window cols(Comparator<? super Row> comparator,
                               Function<? super List<Row>, ? extends T> mapper,
                               Consumer<Window.Cols<T>> config) {
            config.accept(new Window.Cols<>(comparator, mapper));
            return this;
        }
        
        void accept(List<LinkedRow> rows) {
            // TODO: Process unordered window functions first (preserve original encounter order).
            windowFnChildren.forEach(child -> child.accept(rows));
        }
        
        private abstract static class KeyMapper {
            abstract void accept(Row row, Object[] arr);
        }
        
        private abstract static class WindowFunction {
            abstract void accept(List<LinkedRow> rows);
        }
        
        private class KeyRowMapper extends KeyMapper {
            final Function<? super Row, ?> mapper;
            final int localIndex;
            
            KeyRowMapper(int localIndex, Function<? super Row, ?> mapper) {
                this.localIndex = localIndex;
                this.mapper = mapper;
            }
    
            @Override
            void accept(Row row, Object[] arr) {
                arr[localIndex] = mapper.apply(row);
            }
            
            void addToParent() {
                Window.this.keyMapperChildren.add(this);
            }
        }
        
        private class ColRowMapper<T> extends WindowFunction {
            final int index;
            final Comparator<? super Row> comparator;
            final BiConsumer<? super List<Row>, ? super Consumer<T>> mapper;
            
            ColRowMapper(int index,
                         Comparator<? super Row> comparator,
                         BiConsumer<? super List<Row>, ? super Consumer<T>> mapper) {
                this.index = index;
                this.comparator = comparator;
                this.mapper = mapper;
            }
    
            @Override
            void accept(List<LinkedRow> rows) {
                if (comparator != null)
                    rows.sort(comparator);
                class Sink implements Consumer<T> {
                    int i = 0;
                    public void accept(T it) {
                        // Throws IOOBE
                        rows.get(i++).next[index] = it;
                    }
                    void finish() {
                        if (i == 1) {
                            Object first = rows.get(0).next[index];
                            rows.forEach(row -> row.next[index] = first);
                        } else if (i != rows.size()) {
                            throw new IllegalStateException("Window function must produce exactly one value, or one value per row");
                        }
                    }
                }
                Sink sink = new Sink();
                mapper.accept(Collections.unmodifiableList(rows), sink);
                sink.finish();
            }
            
            Window addToParent() {
                boolean isParentUnseen = Window.this.windowFnChildren.isEmpty();
                Window.this.windowFnChildren.add(this);
                return isParentUnseen ? Window.this : null;
            }
        }
    
        public class Keys<T> extends KeyMapper {
            final Function<? super Row, ? extends T> mapper;
            final List<KeyObjMapper> children = new ArrayList<>();
            
            Keys(Function<? super Row, ? extends T> mapper) {
                this.mapper = mapper;
            }
            
            public Keys<T> key(Function<? super T, ?> mapper) {
                // Since all keys are temps and cannot be overridden, can bypass definitions and indexByColumn,
                // and just add to parent right away. This simplifies much.
                new KeyObjMapper(keyCount++, mapper).addToParent();
                return this;
            }
            
            @Override
            void accept(Row row, Object[] arr) {
                T obj = mapper.apply(row);
                children.forEach(child -> child.accept(obj, arr));
            }
            
            void addToParent() {
                Window.this.keyMapperChildren.add(this);
            }
            
            private class KeyObjMapper {
                final Function<? super T, ?> mapper;
                final int localIndex;
                
                KeyObjMapper(int localIndex, Function<? super T, ?> mapper) {
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
        
        public class Cols<T> extends WindowFunction {
            final Comparator<? super Row> comparator;
            final Function<? super List<Row>, ? extends T> mapper;
            final List<ColObjMapper<?>> children = new ArrayList<>();
            
            Cols(Comparator<? super Row> comparator,
                 Function<? super List<Row>, ? extends T> mapper) {
                this.comparator = comparator;
                this.mapper = mapper;
            }
            
            public <U> Cols<T> col(Column<U> column,
                                      BiConsumer<? super T, ? super Consumer<U>> mapper) {
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                ColObjMapper<U> def = new ColObjMapper<>(index, mapper);
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
                return this;
            }
    
            @Override
            void accept(List<LinkedRow> rows) {
                if (comparator != null)
                    rows.sort(comparator);
                T obj = mapper.apply(Collections.unmodifiableList(rows));
                children.forEach(child -> child.accept(rows, obj));
            }
            
            Window addToParent() {
                boolean isParentUnseen = Window.this.windowFnChildren.isEmpty();
                Window.this.windowFnChildren.add(this);
                return isParentUnseen ? Window.this : null;
            }
            
            private class ColObjMapper<U> {
                final int index;
                final BiConsumer<? super T, ? super Consumer<U>> mapper;
                
                ColObjMapper(int index, BiConsumer<? super T, ? super Consumer<U>> mapper) {
                    this.index = index;
                    this.mapper = mapper;
                }
                
                void accept(List<LinkedRow> rows, T obj) {
                    class Sink implements Consumer<U> {
                        int i = 0;
                        public void accept(U it) {
                            // Throws IOOBE
                            rows.get(i++).next[index] = it;
                        }
                        void finish() {
                            if (i == 1) {
                                Object first = rows.get(0).next[index];
                                rows.forEach(row -> row.next[index] = first);
                            } else if (i != rows.size()) {
                                throw new IllegalStateException("Window function must produce exactly one value, or one value per row");
                            }
                        }
                    }
                    Sink sink = new Sink();
                    mapper.accept(obj, sink);
                    sink.finish();
                }
                
                Window addToParent() {
                    boolean isParentUnseen = Cols.this.children.isEmpty();
                    Cols.this.children.add(this);
                    return isParentUnseen ? Cols.this.addToParent() : null;
                }
            }
        }
        
        // ROW_NUMBER() OVER(PARTITION BY columnA, columnB ORDER BY column C)
        //
        // .window($->$.tempKey(columnA).tempKey(columnB).cols(comparing(row -> row.get(columnC)), $->$.col(rowNumber, list -> IntStream.range(0, list.size()).boxed())));
        // .window($->$
        //     .tempKey(columnA)
        //     .tempKey(columnB)
        //     .cols(comparing(row -> row.get(columnC)), $->$
        //         .col(rowNumber, list -> IntStream.range(0, list.size()).boxed())
        //     )
        // );
        //
        // .window($->$
        //     .partitionBy(columnA, columnB)
        //     .orderBy(columnC, $->$
        //         .col(rowNumber, x -> IntStream.range(0, x.size()).boxed())
        //     )
        // )
        //
        // .window($->$
        //     .partitionBy(columnA, columnB)
        //     .orderBy(columnC, $->$.col(rowNumber, rowNumber())
        // )
        //
        // .window($->$
        //     .keys(columnA, columnB)
        //     .cols(nullsFirst(comparing(columnC::get)), $->$.col(rowNumber, rowNumber()))
        //
        //     .cols(order(header).ascNullsFirst(columnC), ...)
        //
        //     .cols($->$.asc(columnC),
        //           $->$.col(rowNumber, rowNumber())
        //     )
        //
        // rowNumber() { return list -> IntStream.range(0, list.size()).boxed(); }
        // rowNumber() { return (list, rx) -> for (int i = 0; i < list.size(); i++) rx.accept(i); }
        //
        // rank(comp) {
        //     return list -> {
        //         int[] rank = { 1 };
        //         IntStream.concat(IntStream.of(1),
        //                          IntStream.range(1, list.size())
        //                                   .map(i -> comp.apply(list.get(i-1), list.get(i)) == 0
        //                                             ? rank[0] : rank[0] = i+1);
        //     };
        // }
        //
        // rank(comparator) {
        //     return (list, rx) -> {
        //         int rank = 1;
        //         rx.accept(rank);
        //         T prev = list.get(0);
        //         for (int i = 1; i < list.size(); i++) {
        //             int cmp = comparator.apply(prev, prev = list.get(i));
        //             rx.accept(cmp == 0 ? rank : rank = i+1);
        //         }
        //     }
        // }
    }
    
    // Used by window / analytical functions.
    static class LinkedRow extends Row {
        final Object[] next;
        
        LinkedRow(Row curr, Object[] next) {
            super(curr.header, curr.data);
            this.next = next;
        }
    }
}
