package vinyl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        List<Select.Mapper> finalMappers = new ArrayList<>();
    
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
            for (Select.Mapper mapper : finalMappers)
                mapper.accept(lt, rt, arr);
            return new Record(nextHeader, arr);
        };
        Record leftNilRecord = new Record(left.header, new Object[left.header.fields.length]);
        Record rightNilRecord = new Record(right.header, new Object[right.header.fields.length]);
        
        // Step 2: Set up the join operation.
        
        // We lazily create an index over the right side (when the first left record arrives), and stream over the left,
        // matching each left record to right records via the index, and creating an output record from each left-right
        // record pair. If this is a left- or full-join, we emit an output record even if a left record matches no right
        // records, by pairing the left with an all-null record. If this is a right- or full-join, we keep track of
        // unmatched right records, and after the left side is exhausted, emit an output record for each unmatched right
        // record, by pairing the right with an all-null record.
        
        boolean retainUnmatchedRight = type == JoinType.RIGHT || type == JoinType.FULL;
        Lazy<Index> lazyJoiner = new Lazy<>(() -> {
            JoinPred.Simplifier simplifier = new JoinPred.Simplifier();
            pred.accept(simplifier);
            JoinPred simplifiedPred = simplifier.output;
            
            JoinPred.IndexCreator indexCreator = new JoinPred.IndexCreator(right, retainUnmatchedRight);
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
                // For now, Stream.Builder uses SpinedBuffer internally, so eliminates copying if capacity needs increased.
                // And even the SpinedBuffer is avoided if only one element is added.
                Stream.Builder<Record> buffer = Stream.builder();
                lazyJoiner.get().search(record, buffer);
                return buffer.build();
            })
            .onClose(right::close);
        
        if (retainUnmatchedRight) {
            Stream<Record> s = nextStream;
            nextStream = StreamSupport.stream(
                () -> new SwitchingSpliterator<>(new AtomicInteger(0),
                                                 s.spliterator(),
                                                 // If we haven't created the index by the time this is called, it means
                                                 // there was nothing on the left. So entire right side is unmatched.
                                                 () -> (lazyJoiner.isReleasable() ? lazyJoiner.get().unmatchedRight() : right.stream)
                                                     .map(record -> combiner.apply(leftNilRecord, record))
                                                     .spliterator()),
                0, // TODO: Make sure we can still split
                s.isParallel()
            ).onClose(s::close);
        }
        
        return new RecordStream(nextHeader, nextStream);
    }
    
    interface Index {
        void search(Record left, Consumer<Record> rx);
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
        public void search(Record left, Consumer<Record> rx) {
            Consumer<Record> sink = right -> rx.accept(combiner.apply(left, right));
            index.search(left, sink);
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
        public void search(Record left, Consumer<Record> rx) {
            class Sink implements Consumer<Record> {
                boolean noneMatch = true;
                public void accept(Record right) {
                    noneMatch = false;
                    rx.accept(combiner.apply(left, right));
                }
                public void finish() {
                    if (noneMatch)
                        rx.accept(combiner.apply(left, rightNilRecord));
                }
            }
            Sink sink = new Sink();
            index.search(left, sink);
            sink.finish();
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
        public void search(Record left, Consumer<Record> rx) {
            Consumer<FlaggedRecord> sink = right -> {
                right.isJoined = true;
                rx.accept(combiner.apply(left, right.record));
            };
            index.search(left, Utils.cast(sink));
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
        public void search(Record left, Consumer<Record> rx) {
            class Sink implements Consumer<FlaggedRecord> {
                boolean noneMatch = true;
                public void accept(FlaggedRecord right) {
                    noneMatch = false;
                    right.isJoined = true;
                    rx.accept(combiner.apply(left, right.record));
                }
                public void finish() {
                    if (noneMatch)
                        rx.accept(combiner.apply(left, rightNilRecord));
                }
            }
            Sink sink = new Sink();
            index.search(left, Utils.cast(sink));
            sink.finish();
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
    
    // Used by right/full joins to keep track of unmatched records on the right. During the join, records that are
    // matched are marked as joined. Remaining un-joined records are emitted after the join.
    static class FlaggedRecord extends Record {
        final Record record;
        boolean isJoined = false;
        
        FlaggedRecord(Record record) {
            super(record.header, record.values);
            this.record = record;
        }
    }
    
    public class On {
        On() {} // Prevent default public constructor
    
        public <T> JoinExpr<T> left(Field<T> field) {
            FieldPin<T> pin = left.header.pin(field);
            return new JoinExpr.RecordExpr<>(field, JoinAPI.LEFT, pin::get);
        }
        
        public <T> JoinExpr<T> right(Field<T> field) {
            FieldPin<T> pin = right.header.pin(field);
            return new JoinExpr.RecordExpr<>(field, JoinAPI.RIGHT, pin::get);
        }
        
        public <T> JoinExpr<T> left(Function<? super Record, T> mapper) {
            return new JoinExpr.RecordExpr<>(null, JoinAPI.LEFT, mapper);
        }
        
        public <T> JoinExpr<T> right(Function<? super Record, T> mapper) {
            return new JoinExpr.RecordExpr<>(null, JoinAPI.RIGHT, mapper);
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
        
        public JoinPred lmatch(Predicate<? super Record> predicate) {
            return new JoinPred.SideMatch(true, predicate);
        }
        
        public JoinPred rmatch(Predicate<? super Record> predicate) {
            return new JoinPred.SideMatch(false, predicate);
        }
        
        public JoinPred match(BiPredicate<? super Record, ? super Record> predicate) {
            return new JoinPred.Match(predicate);
        }
    
        // TODO
//        public JoinPred like(JoinExpr<String> left, String pattern) {
//            throw new UnsupportedOperationException();
//        }
    }
    
    public class Select {
        private final Map<Field<?>, Integer> indexByField = new HashMap<>();
        private final List<Object> definitions = new ArrayList<>();
        
        Select() {} // Prevent default public constructor
        
        public Select lall() {
            for (Field<?> field : left.header.fields)
                lField(field);
            return this;
        }
    
        public Select rall() {
            for (Field<?> field : right.header.fields)
                rField(field);
            return this;
        }
    
        public Select lallExcept(Field<?>... excluded) {
            Set<Field<?>> excludedSet = Set.of(excluded);
            for (Field<?> field : left.header.fields)
                if (!excludedSet.contains(field))
                    lField(field);
            return this;
        }
    
        public Select rallExcept(Field<?>... excluded) {
            Set<Field<?>> excludedSet = Set.of(excluded);
            for (Field<?> field : right.header.fields)
                if (!excludedSet.contains(field))
                    rField(field);
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select lField(Field<?> field) {
            FieldPin pin = new FieldPin(field, left.header.indexOf(field));
            return field((Field) field, (lt, rt) -> lt.get(pin));
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rField(Field<?> field) {
            FieldPin pin = new FieldPin(field, right.header.indexOf(field));
            return field((Field) field, (lt, rt) -> rt.get(pin));
        }
        
        public <T> Select field(Field<T> field, BiFunction<? super Record, ? super Record, ? extends T> mapper) {
            int index = indexByField.computeIfAbsent(field, k -> definitions.size());
            RecordMapper def = new RecordMapper(index, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        public Select lFields(Field<?>... fields) {
            for (Field<?> field : fields)
                lField(field);
            return this;
        }
        
        public Select rFields(Field<?>... fields) {
            for (Field<?> field : fields)
                rField(field);
            return this;
        }
        
        public <T> Select fields(BiFunction<? super Record, ? super Record, ? extends T> mapper, Consumer<Fields<T>> config) {
            config.accept(new Fields<>(mapper));
            return this;
        }
        
        private abstract class Mapper {
            abstract void accept(Record left, Record right, Object[] arr);
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
        
        public class Fields<T> extends Mapper {
            final BiFunction<? super Record, ? super Record, ? extends T> mapper;
            final List<ObjectMapper> children = new ArrayList<>();
    
            Fields(BiFunction<? super Record, ? super Record, ? extends T> mapper) {
                this.mapper = mapper;
            }
    
            public <U> Fields<T> field(Field<U> field, Function<? super T, ? extends U> mapper) {
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
