package da.tasets;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static da.tasets.UnsafeUtils.cast;

public class JoinAPI {
    private final JoinType type;
    private final DatasetStream left;
    private final DatasetStream right;
    private JoinPred pred = null;
    private Select select = null;
    
    enum JoinType { INNER, LEFT, RIGHT, FULL }
    
    // 'Side' values, used by JoinExpr and JoinPred.
    static int NONE = 0;
    static int RIGHT = 1;
    static int LEFT = 2;
    static int BOTH = 3; // LEFT | RIGHT
    
    JoinAPI(JoinType type, DatasetStream left, DatasetStream right) {
        this.type = type;
        this.left = left;
        this.right = right;
    }
    
    public JoinAPI on(Function<On, JoinPred> config) {
        pred = config.apply(new On());
        return this;
    }
    
    public JoinAPI andSelect(Consumer<Select> config) {
        config.accept(select = new Select());
        return this;
    }
    
    DatasetStream accept(Consumer<JoinAPI> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        
        JoinPred finalPred = pred != null ? pred : new On().all(); // cross-join
        Select finalSelect = select != null ? select : new Select().lall().rall(); // merge columns
        
        // Step 1: Set up the new header, and the combiner (that produces a new row from a left and a right row).
        
        // Final mappers will combine ObjMapper children into their parent.
        Map<Column<?>, Integer> finalIndexByColumn = Map.copyOf(finalSelect.indexByColumn);
        List<Select.Mapper> finalMappers = new ArrayList<>();
    
        for (Object definition : finalSelect.definitions) {
            if (definition instanceof Select.RowMapper)
                finalMappers.add((Select.RowMapper) definition);
            else {
                assert definition instanceof Select.Cols.ObjMapper;
                Select.Cols<?>.ObjMapper def = (Select.Cols<?>.ObjMapper) definition;
                Select.Cols<?> parent = def.addToParent();
                if (parent != null)
                    finalMappers.add(parent);
            }
        }
    
        int size = finalIndexByColumn.size();
        Header nextHeader = new Header(finalIndexByColumn);
        BinaryOperator<Row> combiner = (lt, rt) -> {
            Object[] arr = new Object[size];
            for (Select.Mapper mapper : finalMappers)
                mapper.accept(lt, rt, arr);
            return new Row(nextHeader, arr);
        };
        Row leftNilRow = new Row(left.header, new Object[left.header.columns.length]);
        Row rightNilRow = new Row(right.header, new Object[right.header.columns.length]);
        
        // Step 2: Set up the join operation.
        
        // We lazily create an index over the right side (when the first left row arrives), and stream over the left,
        // matching each left row to right rows via the index, and creating an output row from each left-right row pair.
        // If this is a left- or full-join, we emit an output row even if a left row matches no right rows, by pairing
        // the left with an all-null row. If this is a right- or full-join, we keep track of unmatched right rows, and
        // after the left side is exhausted, emit an output row for each unmatched right row, by pairing the right with
        // an all-null row.
        
        boolean retainUnmatchedRight = type == JoinType.RIGHT || type == JoinType.FULL;
        Lazy<Index> lazyJoiner = new Lazy<>(() -> {
            JoinPred.Simplifier simplifier = new JoinPred.Simplifier();
            finalPred.accept(simplifier);
            JoinPred simplifiedPred = simplifier.output;
            
            JoinPred.IndexCreator indexCreator = new JoinPred.IndexCreator(right, retainUnmatchedRight);
            simplifiedPred.accept(indexCreator);
            Index index = indexCreator.output;
            
            switch (type) {
                case INNER: return new InnerJoiner(index, combiner);
                case LEFT:  return new LeftJoiner( index, combiner, rightNilRow);
                case RIGHT: return new RightJoiner(index, combiner, leftNilRow);
                case FULL:  return new FullJoiner( index, combiner, leftNilRow, rightNilRow);
                default: throw new AssertionError(); // unreachable
            }
        });
        Stream<Row> nextStream = left.stream
            .flatMap(row -> {
                // TODO: For Java 16+, optimize by using mapMulti() instead of flatMap().
                List<Row> buffer = new ArrayList<>();
                lazyJoiner.get().search(row, buffer::add);
                return buffer.stream();
            })
            .onClose(right::close);
        
        if (retainUnmatchedRight) {
            Stream<Row> s = nextStream;
            nextStream = StreamSupport.stream(
                () -> new SwitchingSpliterator<>(new AtomicInteger(0),
                                                 s.spliterator(),
                                                 // If we haven't created the index by the time this is called, it means
                                                 // there was nothing on the left. So entire right side is unmatched.
                                                 () -> (lazyJoiner.isReleasable() ? lazyJoiner.get().unmatchedRight() : right.stream)
                                                     .map(row -> combiner.apply(leftNilRow, row))
                                                     .spliterator()),
                0, // TODO: Make sure we can still split
                s.isParallel()
            ).onClose(s::close);
        }
        
        return new DatasetStream(nextHeader, nextStream);
    }
    
    interface Index {
        void search(Row left, Consumer<Row> rx);
        default Stream<Row> unmatchedRight() { throw new UnsupportedOperationException(); }
    }
    
    private static class InnerJoiner implements Index {
        final Index index;
        final BinaryOperator<Row> combiner;
        
        InnerJoiner(Index index, BinaryOperator<Row> combiner) {
            this.index = index;
            this.combiner = combiner;
        }
        
        @Override
        public void search(Row left, Consumer<Row> rx) {
            Consumer<Row> sink = right -> rx.accept(combiner.apply(left, right));
            index.search(left, sink);
        }
    }
    
    private static class LeftJoiner implements Index {
        final Index index;
        final BinaryOperator<Row> combiner;
        final Row rightNilRow;
    
        LeftJoiner(Index index, BinaryOperator<Row> combiner, Row rightNilRow) {
            this.index = index;
            this.combiner = combiner;
            this.rightNilRow = rightNilRow;
        }
    
        @Override
        public void search(Row left, Consumer<Row> rx) {
            class Sink implements Consumer<Row> {
                boolean noneMatch = true;
                public void accept(Row right) {
                    noneMatch = false;
                    rx.accept(combiner.apply(left, right));
                }
                public void finish() {
                    if (noneMatch)
                        rx.accept(combiner.apply(left, rightNilRow));
                }
            }
            Sink sink = new Sink();
            index.search(left, sink);
            sink.finish();
        }
    }
    
    private static class RightJoiner implements Index {
        final Index index;
        final BinaryOperator<Row> combiner;
        final Row leftNilRow;
    
        RightJoiner(Index index,
                    BinaryOperator<Row> combiner,
                    Row leftNilRow) {
            this.index = index;
            this.combiner = combiner;
            this.leftNilRow = leftNilRow;
        }
        
        @Override
        public void search(Row left, Consumer<Row> rx) {
            Consumer<FlaggedRow> sink = right -> {
                right.isJoined = true;
                rx.accept(combiner.apply(left, right.row));
            };
            index.search(left, cast(sink));
        }
        
        @Override
        public Stream<Row> unmatchedRight() {
            return index.unmatchedRight();
        }
    }
    
    private static class FullJoiner implements Index {
        final Index index;
        final BinaryOperator<Row> combiner;
        final Row leftNilRow;
        final Row rightNilRow;
    
        FullJoiner(Index index,
                   BinaryOperator<Row> combiner,
                   Row leftNilRow,
                   Row rightNilRow) {
            this.index = index;
            this.combiner = combiner;
            this.leftNilRow = leftNilRow;
            this.rightNilRow = rightNilRow;
        }
    
        @Override
        public void search(Row left, Consumer<Row> rx) {
            class Sink implements Consumer<FlaggedRow> {
                boolean noneMatch = true;
                public void accept(FlaggedRow right) {
                    noneMatch = false;
                    right.isJoined = true;
                    rx.accept(combiner.apply(left, right.row));
                }
                public void finish() {
                    if (noneMatch)
                        rx.accept(combiner.apply(left, rightNilRow));
                }
            }
            Sink sink = new Sink();
            index.search(left, cast(sink));
            sink.finish();
        }
    
        @Override
        public Stream<Row> unmatchedRight() {
            return index.unmatchedRight();
        }
    }
    
    private static class SwitchingSpliterator<T> implements Spliterator<T> {
        final AtomicInteger splits;
        final Spliterator<T> delegate;
        final Supplier<Spliterator<T>> nextSupplier;
        Spliterator<T> next = null;
        
        SwitchingSpliterator(AtomicInteger splits,
                             Spliterator<T> delegate,
                             Supplier<Spliterator<T>> nextSupplier) {
            this.splits = splits;
            this.delegate = delegate;
            this.nextSupplier = nextSupplier;
            splits.incrementAndGet();
        }
        
        @Override
        public int characteristics() {
            return 0;
        }
        
        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }
        
        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            if (next == null) {
                delegate.forEachRemaining(action);
                next = (splits.decrementAndGet() == 0) ? nextSupplier.get() : Spliterators.emptySpliterator();
            }
            next.forEachRemaining(action);
        }
        
        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (next == null) {
                if (delegate.tryAdvance(action))
                    return true;
                next = (splits.decrementAndGet() == 0) ? nextSupplier.get() : Spliterators.emptySpliterator();
            }
            return next.tryAdvance(action);
        }
    
        @Override
        public Spliterator<T> trySplit() {
            if (next == null) {
                // If we are here, we may create a new split and increment. But we can only be here if we have not yet
                // exhausted the current split and decremented. This ensures splits will never become positive again
                // after becoming 0, which ensures we call the supplier at most once.
                Spliterator<T> split = delegate.trySplit();
                return split != null ? new SwitchingSpliterator<>(splits, split, nextSupplier) : null;
            }
            // It would be unusual to reach this point, because it means that both:
            //  1. After exhausting this split, this split determined that it should take ownership of next
            //  2. After exhausting this split, trySplit() was called
            // But if that happens, there's no harm in calling trySplit() on next.
            return next.trySplit();
        }
    }
    
    // Used for left and inner joins, to defer creating the right side until we know left side is non-empty.
    private static class Lazy<T> implements ForkJoinPool.ManagedBlocker {
        private static final Object UNINITIALIZED = new Object();
        private static final VarHandle INSTANCE;
        static {
            try {
                INSTANCE = MethodHandles.lookup().findVarHandle(Lazy.class, "instance", Object.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    
        // This class implements a form of double-checked locking, with the further optimization that we minimize
        // volatile/expensive accesses. We also implement ManagedBlocker, as we expect to (only) be called from inside
        // stream operations, which run in a ForkJoinPool when parallel.
    
        final ReentrantLock lock = new ReentrantLock();
        Supplier<T> supplier;
        Object instance = UNINITIALIZED;
        
        Lazy(Supplier<T> supplier) {
            this.supplier = supplier;
        }
        
        @Override
        public boolean block() {
            if (isReleasable())
                return true;
            lock.lock();
            try {
                if (instance == UNINITIALIZED) {
                    T val = supplier.get();
                    supplier = null; // Free for gc
                    INSTANCE.setRelease(this, val);
                }
            } finally {
                lock.unlock();
            }
            return true;
        }
        
        @Override
        public boolean isReleasable() {
            return instance != UNINITIALIZED;
        }
        
        @SuppressWarnings("unchecked")
        T get() {
            if (isReleasable())
                return (T) instance;
            try {
                // Note that managedBlock() does a volatile read before calling isReleasable(),
                // improving the chance that we see a write from another thread and skip the lock.
                ForkJoinPool.managedBlock(this);
            } catch (InterruptedException unreachable) { // our block() does not throw InterruptedException
                throw new AssertionError();
            }
            return (T) instance;
        }
    }
    
    // Used by right/full joins to keep track of unmatched rows on the right. During the join, rows that are matched are
    // marked as joined. Remaining un-joined rows are emitted after the join.
    static class FlaggedRow extends Row {
        final Row row;
        boolean isJoined = false;
        
        FlaggedRow(Row row) {
            super(row.header, row.data);
            this.row = row;
        }
    }
    
    public class On {
        On() {} // Prevent default public constructor
    
        public <T> JoinExpr<T> left(Column<T> column) {
            Locator<T> locator = left.header.locator(column);
            return new JoinExpr.RowExpr<>(column, JoinAPI.LEFT, locator::get);
        }
        
        public <T> JoinExpr<T> right(Column<T> column) {
            Locator<T> locator = right.header.locator(column);
            return new JoinExpr.RowExpr<>(column, JoinAPI.RIGHT, locator::get);
        }
        
        public <T> JoinExpr<T> left(Function<? super Row, T> mapper) {
            return new JoinExpr.RowExpr<>(null, JoinAPI.LEFT, mapper);
        }
        
        public <T> JoinExpr<T> right(Function<? super Row, T> mapper) {
            return new JoinExpr.RowExpr<>(null, JoinAPI.RIGHT, mapper);
        }
        
        public <T> JoinExpr<T> eval(Supplier<T> supplier) {
            return new JoinExpr.Expr<>(supplier);
        }
        
        public <T> JoinExpr<T> val(T val) {
            return new JoinExpr.Expr<>(() -> val);
        }
    
        public JoinPred eq(JoinExpr<?> left, JoinExpr<?> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.EQ, left, right);
        }
    
        public JoinPred neq(JoinExpr<?> left, JoinExpr<?> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.NEQ, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred gt(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.GT, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred gte(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.GTE, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred lt(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.LT, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred lte(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.LTE, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred between(JoinExpr<T> left, JoinExpr<T> start, JoinExpr<T> end) {
            return new JoinPred.AnyAll(false, List.of(gte(left, start), lte(left, end)));
        }
    
        public JoinPred not(JoinPred predicate) {
            return new JoinPred.Not(predicate);
        }
    
        public JoinPred any(JoinPred... predicates) {
            return new JoinPred.AnyAll(true, List.of(predicates));
        }
    
        public JoinPred all(JoinPred... predicates) {
            return new JoinPred.AnyAll(false, List.of(predicates));
        }
        
        public JoinPred lmatch(Predicate<? super Row> predicate) {
            return new JoinPred.SideMatch(true, predicate);
        }
        
        public JoinPred rmatch(Predicate<? super Row> predicate) {
            return new JoinPred.SideMatch(false, predicate);
        }
        
        public JoinPred match(BiPredicate<? super Row, ? super Row> predicate) {
            return new JoinPred.Match(predicate);
        }
    
        // TODO
//        public JoinPred like(JoinExpr<String> left, String pattern) {
//            throw new UnsupportedOperationException();
//        }
    }
    
    public class Select {
        private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
        private final List<Object> definitions = new ArrayList<>();
        
        Select() {} // Prevent default public constructor
        
        public Select lall() {
            for (Column<?> column : left.header.columns)
                lcol(column);
            return this;
        }
    
        public Select rall() {
            for (Column<?> column : right.header.columns)
                rcol(column);
            return this;
        }
    
        public Select lallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            for (Column<?> column : left.header.columns)
                if (!excludedSet.contains(column))
                    lcol(column);
            return this;
        }
    
        public Select rallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            for (Column<?> column : right.header.columns)
                if (!excludedSet.contains(column))
                    rcol(column);
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select lcol(Column<?> column) {
            Locator locator = new Locator(column, left.header.indexOf(column));
            return col((Column) column, (lt, rt) -> lt.get(locator));
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rcol(Column<?> column) {
            Locator locator = new Locator(column, right.header.indexOf(column));
            return col((Column) column, (lt, rt) -> rt.get(locator));
        }
        
        public <T> Select col(Column<T> column, BiFunction<? super Row, ? super Row, ? extends T> mapper) {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        public Select lcols(Column<?>... columns) {
            for (Column<?> column : columns)
                lcol(column);
            return this;
        }
        
        public Select rcols(Column<?>... columns) {
            for (Column<?> column : columns)
                rcol(column);
            return this;
        }
        
        public <T> Select cols(BiFunction<? super Row, ? super Row, ? extends T> mapper, Consumer<Cols<T>> config) {
            config.accept(new Cols<>(mapper));
            return this;
        }
        
        private abstract static class Mapper {
            abstract void accept(Row left, Row right, Object[] arr);
        }
    
        private static class RowMapper extends Mapper {
            final int index;
            final BiFunction<? super Row, ? super Row, ?> mapper;
        
            RowMapper(int index, BiFunction<? super Row, ? super Row, ?> mapper) {
                this.index = index;
                this.mapper = mapper;
            }
        
            @Override
            void accept(Row left, Row right, Object[] arr) {
                arr[index] = mapper.apply(left, right);
            }
        }
        
        public class Cols<T> extends Mapper {
            final BiFunction<? super Row, ? super Row, ? extends T> mapper;
            final List<ObjMapper> children = new ArrayList<>();
    
            Cols(BiFunction<? super Row, ? super Row, ? extends T> mapper) {
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
            void accept(Row left, Row right, Object[] arr) {
                T obj = mapper.apply(left, right);
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
    }
}
