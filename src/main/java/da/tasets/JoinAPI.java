package da.tasets;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.IntStream;

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
        
        if (pred == null)
            pred = new On().all(); // cross-join
        if (select == null)
            select = new Select().lall().rall(); // merge columns
        
        // left/right/const | ineq/eq
        //
        // right-eq-left           --> point-index right
        // right-ineq-left         --> range-index right
        // right-in/eq-const/right --> pre-filter right
        // rmatch                  --> pre-filter right
        // left-in/eq-const/left   --> filter left
        // lmatch                  --> filter left
        // match                   --> filter both
        //
        // all --> nest consecutive indexes & pre-filters, until match() is reached
        // any --> separate consecutive indexes & pre-filters
        //
        // 1. Describe how to build structure on the right-hand side.
        // 2. Describe how to traverse structure and run checks.
        //
        // TODO:
        //  1. Reused point-indexes: L(B) = R(C) AND ... AND L(C) = R(C)
        //     -->   L(B) = R(C) = L(C)    [index on R(C) is reused]
        //  2. Reused range-indexes: L(B) < R(C) AND ... AND L(C) > R(C)
        //     -->   L(B) < R(C) < L(C)    [index on R(C) is reused]
        //
        // left/inner  - conditionally create right (TODO: if not created, when to close right stream?)
        // right/outer - unconditionally create right (always close right stream), concat remainder after left
        
        // .flatMap(left -> right.get().match(left))
        //
        // class Joiner {
        //     ??? index;
        //     // These 2 iff right or full join.
        //     IndexedRow[] rightRows;
        //     IndexSet indices; // TODO: Remove? Could use eg FlaggedRows?
        //     void search(Row left, Consumer<Row> rx) {
        //
        //         // Inner Join
        //         Consumer<Row> wrapper = right -> rx.accept(combine(left, right));
        //         index.search(left, wrapper);
        //
        //         // Right Join
        //         Consumer<IndexedRow> wrapper = right -> {
        //             indices.remove(right.index);
        //             rightRows[right.index] = null;
        //             rx.accept(combine(left, right));
        //         };
        //         index.search(left, cast(wrapper));
        //
        //         // Left Join
        //         class CountingConsumer implements Consumer<Row> {
        //             boolean noneMatch = true;
        //             void accept(Row right) {
        //                 noneMatch = false;
        //                 rx.accept(combine(left, right));
        //             }
        //             void finish() {
        //                 if (noneMatch)
        //                     rx.accept(combine(left, NIL_ROW));
        //             }
        //         }
        //         CountingConsumer wrapper = new CountingConsumer();
        //         index.search(left, wrapper);
        //         wrapper.finish();
        //
        //         // Full Join
        //         class CountingConsumer implements Consumer<IndexedRow> {
        //             boolean noneMatch = true;
        //             void accept(Row right) {
        //                 noneMatch = false;
        //                 indices.remove(right.index);
        //                 rightRows[right.index] = null;
        //                 rx.accept(combine(left, right));
        //             }
        //             void finish() {
        //                 if (noneMatch)
        //                     rx.accept(combine(left, NIL_ROW));
        //             }
        //         }
        //         CountingConsumer wrapper = new CountingConsumer();
        //         index.search(left, cast(wrapper));
        //         wrapper.finish();
        //     }
        //
        //     Spliterator<Row> remainingRight() {
        //         return Array.stream(rightRows).filter(row -> !row.joined).spliterator();
        //     }
        // }
        
        // TODO
        throw new UnsupportedOperationException();
    }
    
    
    // TODO: Closing probably doesn't work at all if someone calls stream.limit(N) on the wrapper Stream.
    //  ie, knowing whether the spliterator finished is insufficient for determining whether the right stream should be closed.
    //  We probably just need to do a composed close, stupid as it is.
    
    // For left/inner join, use nextSupplier to signal termination, and close the right stream if not already closed.
    // For right/full join, use nextSupplier to switch to spliterator over remaining from right side.
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
    private static class LazyBox<T> implements ForkJoinPool.ManagedBlocker {
        private static final Object UNINITIALIZED = new Object();
        private static final VarHandle INSTANCE;
        static {
            try {
                INSTANCE = MethodHandles.lookup().findVarHandle(LazyBox.class, "instance", Object.class);
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
        
        LazyBox(Supplier<T> supplier) {
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
            } catch (InterruptedException unreachable) { // our block() does not throw this
                throw new AssertionError();
            }
            return (T) instance;
        }
    }
    
    private static class IndexSet {
        private static final VarHandle AA = MethodHandles.arrayElementVarHandle(long[].class);
        
        final long[] words;
        
        // TODO: Optimize? We currently reuse BitSet.stream() for convenience, but that requires initializing all bits
        //  in our array to 1 (BitSet.stream() looks for set bits, not clear bits), and then copying the array to a
        //  BitSet at the end.
    
        IndexSet(int size) {
            int wordSize = ((size - 1) >> 6) + 1;
            words = new long[wordSize];
            if (wordSize != 0) {
                IntStream.range(0, wordSize).parallel().forEach(i -> words[i] = -1L); // set all bits
                words[wordSize - 1] = -1L >>> (64 - size);
            }
        }
        
        public void remove(int bitIndex) {
            int wordIndex = bitIndex >> 6;
            long oldWord = words[wordIndex]; // optimistic plain read
            long newWord = oldWord & ~(1L << bitIndex);
            while (newWord != oldWord && oldWord != (oldWord = (long) AA.compareAndExchange(words, wordIndex, oldWord, newWord))) {
                newWord = oldWord & ~(1L << bitIndex);
            }
        }
        
        public IntStream stream() {
            return BitSet.valueOf(words).stream();
        }
    }
    
    // Used by right/full joins to keep track of unmatched rows on the right. IndexedRows are put into an array at the
    // beginning of the join, which defines their index. During the join, rows that are matched are "removed" from the
    // array (conceptually; really their index is removed from a bitset). Remaining rows are emitted after the join.
    private static class IndexedRow extends Row {
        int index;
        
        IndexedRow(Row row) {
            super(row.header, row.data);
        }
    }
    
    public class On {
        On() {} // Prevent default public constructor
    
        public <T> JoinExpr<T> left(Column<T> column) {
            return new JoinExpr.LCol<>(left.header.locator(column));
        }
        
        public <T> JoinExpr<T> right(Column<T> column) {
            return new JoinExpr.RCol<>(right.header.locator(column));
        }
        
        public <T> JoinExpr<T> left(Function<? super Row, T> mapper) {
            return new JoinExpr.LExpr<>(mapper);
        }
        
        public <T> JoinExpr<T> right(Function<? super Row, T> mapper) {
            return new JoinExpr.RExpr<>(mapper);
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
        
                void addToParent() {
                    Cols.this.children.add(this);
                }
            }
        }
    }
}
