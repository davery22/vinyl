package vinyl;

import vinyl.Record.FlaggedRecord;
import vinyl.Record.NilRecord;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A configurator used to define a relational join operation between a left and a right
 * {@link RecordStream record-stream}.
 *
 * <p>A join operation streams the left-side, and for each left-side record, searches the right-side for matching
 * records such that the (left, right) pair of records satisfy a configured join condition. When a match is found, a
 * resulting record is created as configured and emitted downstream. The returned stream will have a new header, shared
 * by the new records.
 *
 * <p>The configurator consists of two sub-configurators: a {@link JoinAPI.On} that defines the join condition, and a
 * {@link JoinAPI.Select} that defines the output fields.
 *
 * @see RecordStream#join
 * @see RecordStream#leftJoin
 * @see RecordStream#rightJoin
 * @see RecordStream#fullJoin
 */
public class JoinAPI {
    private final JoinType type;
    private final RecordStream left;
    private final RecordStream right;
    
    enum JoinType { INNER, LEFT, RIGHT, FULL }
    
    // 'Side' values, used by JoinExpr and JoinPred.
    static int NONE = 0;
    static int RIGHT = 1;
    static int LEFT = 2;
    static int BOTH = 3; // LEFT | RIGHT
    
    JoinAPI(JoinType type, RecordStream left, RecordStream right) {
        this.type = type;
        this.left = left;
        this.right = right;
    }
    
    RecordStream accept(Function<JoinAPI.On, JoinPred> onConfig,
                        Consumer<JoinAPI.Select> selectConfig) {
        JoinPred pred = onConfig.apply(new On());
        Select select = new Select();
        selectConfig.accept(select);
        
        // Step 1: Set up the new header, and the combiner (that produces a new record from a left and a right record).
    
        // Avoid picking up side-effects from bad-actor callbacks.
        // Final mappers will combine ObjectMapper children into their parent.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(select.indexByField);
        List<Mapper> finalMappers = new ArrayList<>();
    
        for (Object definition : select.definitions) {
            if (definition instanceof Select.RecordMapper)
                finalMappers.add((Select.RecordMapper) definition);
            else {
                assert definition instanceof Select.Fields.ObjectMapper;
                Select.Fields<?>.ObjectMapper def = (Select.Fields<?>.ObjectMapper) definition;
                Select.Fields<?> parent = def.addToParent();
                if (parent != null)
                    finalMappers.add(parent);
            }
        }
    
        int size = finalIndexByField.size();
        Header nextHeader = new Header(finalIndexByField);
        BinaryOperator<Record> combiner = (lt, rt) -> {
            Object[] arr = new Object[size];
            for (Mapper mapper : finalMappers)
                mapper.accept(lt, rt, arr);
            return new Record(nextHeader, arr);
        };
        Record leftNilRecord = new NilRecord(left.header, new Object[left.header.fields.length]);
        Record rightNilRecord = new NilRecord(right.header, new Object[right.header.fields.length]);
        
        // Step 2: Set up the join operation.
        
        // We lazily create an index over the right side (when the first left record arrives), and stream over the left,
        // matching each left record to right records via the index, and creating an output record from each left-right
        // record pair. If this is a left- or full-join, we emit an output record even if a left record matches no right
        // records, by pairing the left with an all-null record. If this is a right- or full-join, we keep track of
        // unmatched right records, and after the left side is exhausted, emit an output record for each unmatched right
        // record, by pairing the right with an all-null record.
    
        // Chain a no-op, so that the stream will throw if re-used after this call.
        RecordStream rStream = right.peek(it -> {});
        boolean retainUnmatchedRight = type == JoinType.RIGHT || type == JoinType.FULL;
        Lazy<Index> lazyJoiner = new Lazy<>(() -> {
            JoinPred.Simplifier simplifier = new JoinPred.Simplifier();
            pred.accept(simplifier);
            JoinPred simplifiedPred = simplifier.output;
            
            JoinPred.IndexCreator indexCreator = new JoinPred.IndexCreator(rStream, retainUnmatchedRight);
            simplifiedPred.accept(indexCreator);
            Index index = indexCreator.output;
            
            switch (type) {
                case INNER: return new InnerJoiner(index, combiner);
                case LEFT:  return new LeftJoiner(index, combiner, rightNilRecord);
                case RIGHT: return new RightJoiner(index, combiner, leftNilRecord);
                case FULL:  return new FullJoiner(index, combiner, leftNilRecord, rightNilRecord);
                default: throw new AssertionError(); // unreachable
            }
        });
        Stream<Record> nextStream = left.stream
            .flatMap(record -> {
                // TODO: For Java 16+, optimize by using mapMulti() instead of flatMap().
                // For now, Stream.Builder uses SpinedBuffer internally, so eliminates copying if capacity needs
                // increased. And even the SpinedBuffer is avoided if only one element is added.
                Stream.Builder<Record> buffer = Stream.builder();
                lazyJoiner.get().search(record, buffer);
                return buffer.build();
            })
            .onClose(rStream::close);
        
        if (retainUnmatchedRight) {
            Stream<Record> s = nextStream;
            nextStream = StreamSupport.stream(
                () -> new SwitchingSpliterator<>(new AtomicInteger(0),
                                                 s.spliterator(),
                                                 // If we haven't created the index by the time this is called, it means
                                                 // there was nothing on the left. So entire right side is unmatched.
                                                 () -> (lazyJoiner.isReleasable() ? lazyJoiner.get().unmatchedRight() : rStream.stream)
                                                     .map(record -> combiner.apply(leftNilRecord, record))
                                                     .spliterator()),
                0,
                s.isParallel()
            ).onClose(s::close);
        }
        
        return new RecordStream(nextHeader, nextStream);
    }
    
    interface Index {
        void search(Record left, Consumer<Record> sink);
        default Stream<Record> unmatchedRight() { throw new UnsupportedOperationException(); }
    }
    
    private static class InnerJoiner implements Index {
        final Index index;
        final BinaryOperator<Record> combiner;
        
        InnerJoiner(Index index, BinaryOperator<Record> combiner) {
            this.index = index;
            this.combiner = combiner;
        }
        
        @Override
        public void search(Record left, Consumer<Record> sink) {
            Consumer<Record> wrapperSink = right -> sink.accept(combiner.apply(left, right));
            index.search(left, wrapperSink);
        }
    }
    
    private static class LeftJoiner implements Index {
        final Index index;
        final BinaryOperator<Record> combiner;
        final Record rightNilRecord;
    
        LeftJoiner(Index index, BinaryOperator<Record> combiner, Record rightNilRecord) {
            this.index = index;
            this.combiner = combiner;
            this.rightNilRecord = rightNilRecord;
        }
    
        @Override
        public void search(Record left, Consumer<Record> sink) {
            class Sink implements Consumer<Record> {
                boolean noneMatch = true;
                public void accept(Record right) {
                    noneMatch = false;
                    sink.accept(combiner.apply(left, right));
                }
                public void finish() {
                    if (noneMatch)
                        sink.accept(combiner.apply(left, rightNilRecord));
                }
            }
            Sink wrapperSink = new Sink();
            index.search(left, wrapperSink);
            wrapperSink.finish();
        }
    }
    
    private static class RightJoiner implements Index {
        final Index index;
        final BinaryOperator<Record> combiner;
        final Record leftNilRecord;
    
        RightJoiner(Index index,
                    BinaryOperator<Record> combiner,
                    Record leftNilRecord) {
            this.index = index;
            this.combiner = combiner;
            this.leftNilRecord = leftNilRecord;
        }
        
        @Override
        public void search(Record left, Consumer<Record> sink) {
            Consumer<FlaggedRecord> wrapperSink = right -> {
                right.isMatched = true;
                sink.accept(combiner.apply(left, right.record));
            };
            index.search(left, Utils.cast(wrapperSink));
        }
        
        @Override
        public Stream<Record> unmatchedRight() {
            return index.unmatchedRight();
        }
    }
    
    private static class FullJoiner implements Index {
        final Index index;
        final BinaryOperator<Record> combiner;
        final Record leftNilRecord;
        final Record rightNilRecord;
    
        FullJoiner(Index index,
                   BinaryOperator<Record> combiner,
                   Record leftNilRecord,
                   Record rightNilRecord) {
            this.index = index;
            this.combiner = combiner;
            this.leftNilRecord = leftNilRecord;
            this.rightNilRecord = rightNilRecord;
        }
    
        @Override
        public void search(Record left, Consumer<Record> sink) {
            class Sink implements Consumer<FlaggedRecord> {
                boolean noneMatch = true;
                public void accept(FlaggedRecord right) {
                    noneMatch = false;
                    right.isMatched = true;
                    sink.accept(combiner.apply(left, right.record));
                }
                public void finish() {
                    if (noneMatch)
                        sink.accept(combiner.apply(left, rightNilRecord));
                }
            }
            Sink wrapperSink = new Sink();
            index.search(left, Utils.cast(wrapperSink));
            wrapperSink.finish();
        }
    
        @Override
        public Stream<Record> unmatchedRight() {
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
    
    // Used to defer creating the right side index until we know the left side is non-empty.
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
    
    /**
     * A sub-configurator used to define the join condition of a relational join operation. The join condition is
     * encoded as a {@link JoinPred} predicate. The strategy for evaluating the join condition is determined by how the
     * join condition is defined, enabling optimizations. In general, the following rules apply:
     *
     * <ol>
     *     <li>If a predicate does not depend on left or right-side records, it may be evaluated as a pre-join check
     *     <li>If a predicate depends on only left or only right-side records, it may be evaluated as a pre-join filter
     *     on the corresponding side
     *     <li>If a predicate compares left-side records to right-side records via a supported comparison operator, it
     *     may be evaluated by creating an index over the right-side, and searching the index with each left-side record
     *     <li>If a predicate is composed of multiple sub-predicates (via {@link #any any} / {@link #all all}), a more
     *     complex evaluation strategy may be used, eg combinations of right-side indexes and filters
     *     <li>If no other optimization applies, a predicate may be evaluated by looping over right-side records for
     *     each left-side record
     * </ol>
     */
    public class On {
        On() {} // Prevent default public constructor
    
        /**
         * Creates a {@link JoinExpr} that evaluates to the value associated with the given field in each left-side
         * record.
         *
         * @param field the field whose associated value is to be returned from each right-side record
         * @return a {@link JoinExpr} that evaluates to the value associated with the given field in each left-side
         * record
         * @param <T> the value type of the field
         * @throws NoSuchElementException if the left-side stream header does not contain the field
         */
        public <T> JoinExpr<T> left(Field<T> field) {
            FieldPin<T> pin = left.header.pin(field);
            return new JoinExpr.RecordExpr<>(field, JoinAPI.LEFT, record -> record.get(pin));
        }
    
        /**
         * Creates a {@link JoinExpr} that evaluates to the value associated with the given field in each right-side
         * record.
         *
         * @param field the field whose associated value is to be returned from each right-side record
         * @return a {@link JoinExpr} that evaluates to the value associated with the given field in each right-side
         * record
         * @param <T> the value type of the field
         * @throws NoSuchElementException if the right-side stream header does not contain the field
         */
        public <T> JoinExpr<T> right(Field<T> field) {
            FieldPin<T> pin = right.header.pin(field);
            return new JoinExpr.RecordExpr<>(field, JoinAPI.RIGHT, record -> record.get(pin));
        }
    
        /**
         * Creates a {@link JoinExpr} that evaluates to the result of applying the given function to each left-side
         * record.
         *
         * @param mapper the function to apply to each left-side record
         * @return a {@link JoinExpr} that evaluates to the result of applying the given function to each left-side
         * record
         * @param <T> the result type of the function
         */
        public <T> JoinExpr<T> left(Function<? super Record, T> mapper) {
            Objects.requireNonNull(mapper);
            return new JoinExpr.RecordExpr<>(null, JoinAPI.LEFT, mapper);
        }
    
        /**
         * Creates a {@link JoinExpr} that evaluates to the result of applying the given function to each right-side
         * record.
         *
         * @param mapper the function to apply to each right-side record
         * @return a {@link JoinExpr} that evaluates to the result of applying the given function to each right-side
         * record
         * @param <T> the result type of the function
         */
        public <T> JoinExpr<T> right(Function<? super Record, T> mapper) {
            Objects.requireNonNull(mapper);
            return new JoinExpr.RecordExpr<>(null, JoinAPI.RIGHT, mapper);
        }
    
        /**
         * Creates a {@link JoinExpr} that evaluates to the result of calling the given supplier. The supplier will be
         * called at most once during the join operation.
         *
         * @param supplier the value-supplying function
         * @return a {@link JoinExpr} that evaluates to the result of calling the given supplier
         * @param <T> the result type of the function
         */
        public <T> JoinExpr<T> eval(Supplier<T> supplier) {
            Objects.requireNonNull(supplier);
            return new JoinExpr.Expr<>(supplier);
        }
    
        /**
         * Creates a {@link JoinExpr} that evaluates to the given value.
         *
         * @param val the value
         * @return a {@link JoinExpr} that evaluates to the given value
         * @param <T> the value type
         */
        public <T> JoinExpr<T> val(T val) {
            return new JoinExpr.Expr<>(() -> val);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression is equal to the value of the right
         * expression, based on {@link Objects#equals(Object, Object)}.
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression is not equal to the value of the
         * right expression
         */
        public JoinPred eq(JoinExpr<?> left, JoinExpr<?> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.EQ, left, right);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression is not equal to the value of the
         * right expression, based on {@link Objects#equals(Object, Object)}.
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression is not equal to the value of the
         * right expression
         */
        public JoinPred neq(JoinExpr<?> left, JoinExpr<?> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.NEQ, left, right);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression compares equal to the value of the
         * right expression, based on the natural ordering of the values (nulls first/lowest).
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression compares equal to the value of the
         * right expression
         * @param <T> the type of the evaluated expressions
         */
        public <T extends Comparable<? super T>> JoinPred ceq(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.CEQ, left, right);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression compares not equal to the value of
         * the right expression, based on the natural ordering of the values (nulls first/lowest).
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression compares not equal to the value of
         * the right expression
         * @param <T> the type of the evaluated expressions
         */
        public <T extends Comparable<? super T>> JoinPred cneq(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.CNEQ, left, right);
        }
        
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression compares greater than the value of
         * the right expression, based on the natural ordering of the values (nulls first/lowest).
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression compares greater than the value of
         * the right expression
         * @param <T> the type of the evaluated expressions
         */
        public <T extends Comparable<? super T>> JoinPred gt(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.GT, left, right);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression compares greater than or equal to
         * the value of the right expression, based on the natural ordering of the values (nulls first/lowest).
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression compares greater than or equal to
         * the value of the right expression
         * @param <T> the type of the evaluated expressions
         */
        public <T extends Comparable<? super T>> JoinPred gte(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.GTE, left, right);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression compares less than the value of
         * the right expression, based on the natural ordering of the values (nulls first/lowest).
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression compares less than the value of
         * the right expression
         * @param <T> the type of the evaluated expressions
         */
        public <T extends Comparable<? super T>> JoinPred lt(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.LT, left, right);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the left expression compares less than or equal to the
         * value of the right expression, based on the natural ordering of the values (nulls first/lowest).
         *
         * @param left the left expression
         * @param right the right expression
         * @return a {@link JoinPred} that passes if the value of the left expression compares less than or equal to the
         * value of the right expression
         * @param <T> the type of the evaluated expressions
         */
        public <T extends Comparable<? super T>> JoinPred lte(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.LTE, left, right);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the value of the test expression lies between the values of the
         * begin and end expressions. More formally, the predicate passes if the value of the test expression compares
         * greater than or equal to the value of the begin expression and less than or equal to the value of the end
         * expression, based on the natural ordering of the values (nulls first/lowest).
         *
         * <p>This method is equivalent to:
         * <pre>{@code
         * all(gte(test, begin), lte(test, end))
         * }</pre>
         *
         * @param test the test expression
         * @param begin the begin expression
         * @param end the end expression
         * @return a {@link JoinPred} that passes if the value of the test expression lies between the values of the
         * begin and end expressions
         * @param <T> the type of the evaluated expressions
         */
        public <T extends Comparable<? super T>> JoinPred between(JoinExpr<T> test, JoinExpr<T> begin, JoinExpr<T> end) {
            return all(gte(test, begin), lte(test, end));
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the given predicate <em>does not</em> pass.
         *
         * @param predicate the given predicate
         * @return a {@link JoinPred} that passes if the given predicate <em>does not</em> pass
         */
        public JoinPred not(JoinPred predicate) {
            Objects.requireNonNull(predicate);
            return new JoinPred.Not(predicate);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if any of the given predicates pass. If no predicates are given, the
         * resultant predicate always fails.
         *
         * @param predicates the given predicates
         * @return a {@link JoinPred} that passes if any of the given predicates pass
         */
        public JoinPred any(JoinPred... predicates) {
            return new JoinPred.AnyAll(true, List.of(predicates));
        }
        
        /**
         * Creates a {@link JoinPred} that passes if all of the given predicates pass. If no predicates are given, the
         * resultant predicate always passes.
         *
         * @param predicates the given predicates
         * @return a {@link JoinPred} that passes if all of the given predicates pass
         */
        public JoinPred all(JoinPred... predicates) {
            return new JoinPred.AnyAll(false, List.of(predicates));
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the given plain predicate over left-side records passes. The
         * resultant predicate inherently depends on left-side records.
         *
         * @param predicate the given plain predicate
         * @return a {@link JoinPred} that passes if the given plain predicate over left-side records passes.
         */
        public JoinPred leftMatch(Predicate<? super Record> predicate) {
            Objects.requireNonNull(predicate);
            return new JoinPred.SideMatch(true, predicate);
        }
        
        /**
         * Creates a {@link JoinPred} that passes if the given plain predicate over right-side records passes. The
         * resultant predicate inherently depends on right-side records.
         *
         * @param predicate the given plain predicate
         * @return a {@link JoinPred} that passes if the given plain predicate over right-side records passes.
         */
        public JoinPred rightMatch(Predicate<? super Record> predicate) {
            Objects.requireNonNull(predicate);
            return new JoinPred.SideMatch(false, predicate);
        }
    
        /**
         * Creates a {@link JoinPred} that passes if the given plain predicate over left and right-side records passes.
         * The resultant predicate inherently depends on both left and right-side records.
         *
         * @param predicate the given plain predicate
         * @return a {@link JoinPred} that passes if the given plain predicate over left and right-side records passes.
         */
        public JoinPred match(BiPredicate<? super Record, ? super Record> predicate) {
            Objects.requireNonNull(predicate);
            return new JoinPred.Match(predicate);
        }
    
        // TODO?
//        public JoinPred like(JoinExpr<String> test, String pattern) {
//            throw new UnsupportedOperationException();
//        }
    }
    
    private abstract static class Mapper {
        abstract void accept(Record left, Record right, Object[] arr);
    }
    
    /**
     * A sub-configurator used to define the output fields of a relational join operation.
     *
     * <p>Fields defined on the configurator (or any sub-configurators) become fields on a resultant
     * {@link Header header}. The header fields are in order of definition on the configurator(s). A field may be
     * redefined on the configurator(s), in which case its definition is replaced, but its original position in the
     * header is retained.
     */
    public class Select {
        private final Map<Field<?>, Integer> indexByField = new HashMap<>();
        private final List<Object> definitions = new ArrayList<>();
        
        Select() {} // Prevent default public constructor
    
        /**
         * Defines (or redefines) each field from the left-side stream as the lookup of the same field on each left-side
         * input record.
         *
         * @return this configurator
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select leftAllFields() {
            Field<?>[] fields = left.header.fields;
            for (int i = 0; i < fields.length; i++) {
                FieldPin pin = new FieldPin(fields[i], i);
                field((Field) fields[i], (lt, rt) -> lt.get(pin));
            }
            return this;
        }
    
        /**
         * Defines (or redefines) each field from the right-side stream as the lookup of the same field on each
         * right-side input record.
         *
         * @return this configurator
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rightAllFields() {
            Field<?>[] fields = right.header.fields;
            for (int i = 0; i < fields.length; i++) {
                FieldPin pin = new FieldPin(fields[i], i);
                field((Field) fields[i], (lt, rt) -> rt.get(pin));
            }
            return this;
        }
    
        /**
         * Defines (or redefines) each field from the left-side stream, excluding the given excluded fields, as the
         * lookup of the same field on each left-side input record.
         *
         * @param excluded the left-side fields to exclude
         * @return this configurator
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select leftAllFieldsExcept(Field<?>... excluded) {
            Set<Field<?>> excludedSet = Set.of(excluded);
            Field<?>[] fields = left.header.fields;
            for (int i = 0; i < fields.length; i++)
                if (!excludedSet.contains(fields[i])) {
                    FieldPin pin = new FieldPin(fields[i], i);
                    field((Field) fields[i], (lt, rt) -> lt.get(pin));
                }
            return this;
        }
    
        /**
         * Defines (or redefines) each field from the right-side stream, excluding the given excluded fields, as the
         * lookup of the same field on each right-side input record.
         *
         * @param excluded the right-side fields to exclude
         * @return this configurator
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rightAllFieldsExcept(Field<?>... excluded) {
            Set<Field<?>> excludedSet = Set.of(excluded);
            Field<?>[] fields = right.header.fields;
            for (int i = 0; i < fields.length; i++)
                if (!excludedSet.contains(fields[i])) {
                    FieldPin pin = new FieldPin(fields[i], i);
                    field((Field) fields[i], (lt, rt) -> rt.get(pin));
                }
            return this;
        }
    
        /**
         * Defines (or redefines) the given field as the lookup of the same field on each left-side input record.
         *
         * @param field the field
         * @return this configurator
         * @throws NoSuchElementException if the left-side stream header does not contain the given field
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select leftField(Field<?> field) {
            FieldPin pin = left.header.pin(field);
            return field((Field) field, (lt, rt) -> lt.get(pin));
        }
    
        /**
         * Defines (or redefines) the given field as the lookup of the same field on each right-side input record.
         *
         * @param field the field
         * @return this configurator
         * @throws NoSuchElementException if the right-side stream header does not contain the given field
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rightField(Field<?> field) {
            FieldPin pin = right.header.pin(field);
            return field((Field) field, (lt, rt) -> rt.get(pin));
        }
    
        /**
         * Defines (or redefines) the given field as the application of the given function to each pair of left and
         * right-side input records.
         *
         * @param field the field
         * @param mapper a function to apply to each pair of left and right-side input records
         * @return this configurator
         * @param <T> the value type of the field
         */
        public <T> Select field(Field<T> field, BiFunction<? super Record, ? super Record, ? extends T> mapper) {
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
         * Defines (or redefines) each of the given fields as the lookup of the same field on each left-side input
         * record.
         *
         * @param fields the fields
         * @return this configurator
         * @throws NoSuchElementException if the left-side stream header does not contain a given field
         */
        public Select leftFields(Field<?>... fields) {
            for (Field<?> field : fields)
                leftField(field);
            return this;
        }
    
        /**
         * Defines (or redefines) each of the given fields as the lookup of the same field on each right-side input
         * record.
         *
         * @param fields the fields
         * @return this configurator
         * @throws NoSuchElementException if the right-side stream header does not contain a given field
         */
        public Select rightFields(Field<?>... fields) {
            for (Field<?> field : fields)
                rightField(field);
            return this;
        }
    
        /**
         * Configures a sub-configurator, that may define (or redefine) fields in terms of the result of applying the
         * given function to each pair of left and right-side input records. If no fields are defined by the
         * sub-configurator, possibly due to later field redefinitions, the entire sub-configurator is discarded.
         *
         * @param mapper a function to apply to each pair of left and right-side input records
         * @param config a consumer of the sub-configurator
         * @return this configurator
         * @param <T> the result type of the function
         */
        public <T> Select fields(BiFunction<? super Record, ? super Record, ? extends T> mapper, Consumer<Fields<T>> config) {
            Objects.requireNonNull(mapper);
            config.accept(new Fields<>(mapper));
            return this;
        }
        
        private class RecordMapper extends Mapper {
            final int index;
            final BiFunction<? super Record, ? super Record, ?> mapper;
        
            RecordMapper(int index, BiFunction<? super Record, ? super Record, ?> mapper) {
                this.index = index;
                this.mapper = mapper;
            }
        
            @Override
            void accept(Record left, Record right, Object[] arr) {
                arr[index] = mapper.apply(left, right);
            }
        }
    
        /**
         * A sub-configurator used to define output fields of a relational join operation that depend on a common
         * intermediate result.
         *
         * @param <T> the intermediate result type
         */
        public class Fields<T> extends Mapper {
            final BiFunction<? super Record, ? super Record, ? extends T> mapper;
            final List<ObjectMapper> children = new ArrayList<>();
    
            Fields(BiFunction<? super Record, ? super Record, ? extends T> mapper) {
                this.mapper = mapper;
            }
    
            /**
             * Defines (or redefines) the given field as the application of the given function to this
             * sub-configurator's intermediate result.
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
            void accept(Record left, Record right, Object[] arr) {
                T obj = mapper.apply(left, right);
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
    }
}
