package da.tasets;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class RecordStream implements Stream<Record> {
    final Header header;
    final Stream<Record> stream;
    
    RecordStream(Header header, Stream<Record> stream) {
        this.header = header;
        this.stream = stream;
    }
    
    // --- new ---
    
    public Header header() {
        return header;
    }
    
    public static RecordStream concat(RecordStream a, RecordStream b) {
        if (!a.header.equals(b.header))
            throw new IllegalArgumentException("Header mismatch");
        return new RecordStream(a.header, Stream.concat(a.stream, b.stream));
    }
    
    @SuppressWarnings("unchecked")
    public static <T> Aux<T> aux(Stream<T> stream) {
        if (stream instanceof RecordStream)
            return (Aux<T>) ((RecordStream) stream).aux();
        if (stream instanceof Aux)
            return (Aux<T>) stream;
        // Chain a no-op, so that the stream will throw if re-used after this call.
        Stream<T> s = stream.peek(it -> {});
        return new Aux<>(s);
    }
    
    public static AuxInt aux(IntStream stream) {
        if (stream instanceof AuxInt)
            return (AuxInt) stream;
        // Chain a no-op, so that the stream will throw if re-used after this call.
        IntStream s = stream.peek(it -> {});
        return new AuxInt(s);
    }
    
    public static AuxLong aux(LongStream stream) {
        if (stream instanceof AuxLong)
            return (AuxLong) stream;
        // Chain a no-op, so that the stream will throw if re-used after this call.
        LongStream s = stream.peek(it -> {});
        return new AuxLong(s);
    }
    
    public static AuxDouble aux(DoubleStream stream) {
        if (stream instanceof AuxDouble)
            return (AuxDouble) stream;
        // Chain a no-op, so that the stream will throw if re-used after this call.
        DoubleStream s = stream.peek(it -> {});
        return new AuxDouble(s);
    }
    
    public RecordSet toRecordSet() {
        return new RecordSet(header, stream.map(record -> record.values).toArray(Object[][]::new));
    }
    
    public Aux<Record> aux() {
        // Chain a no-op, so that the stream will throw if re-used after this call.
        Stream<Record> s = stream.peek(it -> {});
        return new Aux<>(s);
    }
    
    public RecordStream select(Consumer<SelectAPI> config) {
        return new SelectAPI(this).accept(config);
    }
    
    public RecordStream aggregate(Consumer<AggregateAPI> config) {
        return new AggregateAPI(this).accept(config);
    }
    
    public RecordStream join(RecordStream right, Consumer<JoinAPI> config) {
        return new JoinAPI(JoinAPI.JoinType.INNER, this, right).accept(config);
    }
    
    public RecordStream leftJoin(RecordStream right, Consumer<JoinAPI> config) {
        return new JoinAPI(JoinAPI.JoinType.LEFT, this, right).accept(config);
    }
    
    public RecordStream rightJoin(RecordStream right, Consumer<JoinAPI> config) {
        return new JoinAPI(JoinAPI.JoinType.RIGHT, this, right).accept(config);
    }
    
    public RecordStream fullJoin(RecordStream right, Consumer<JoinAPI> config) {
        return new JoinAPI(JoinAPI.JoinType.FULL, this, right).accept(config);
    }
    
    // --- old ---
    
    @Override
    public RecordStream filter(Predicate<? super Record> predicate) {
        return new RecordStream(header, stream.filter(predicate));
    }
    
    @Override
    public <R> Aux<R> map(Function<? super Record, ? extends R> mapper) {
        return new Aux<>(stream.map(mapper));
    }
    
    @Override
    public AuxInt mapToInt(ToIntFunction<? super Record> mapper) {
        return new AuxInt(stream.mapToInt(mapper));
    }
    
    @Override
    public AuxLong mapToLong(ToLongFunction<? super Record> mapper) {
        return new AuxLong(stream.mapToLong(mapper));
    }
    
    @Override
    public AuxDouble mapToDouble(ToDoubleFunction<? super Record> mapper) {
        return new AuxDouble(stream.mapToDouble(mapper));
    }
    
    @Override
    public <R> Aux<R> flatMap(Function<? super Record, ? extends Stream<? extends R>> mapper) {
        return new Aux<>(stream.flatMap(mapper));
    }
    
    @Override
    public AuxInt flatMapToInt(Function<? super Record, ? extends IntStream> mapper) {
        return new AuxInt(stream.flatMapToInt(mapper));
    }
    
    @Override
    public AuxLong flatMapToLong(Function<? super Record, ? extends LongStream> mapper) {
        return new AuxLong(stream.flatMapToLong(mapper));
    }
    
    @Override
    public AuxDouble flatMapToDouble(Function<? super Record, ? extends DoubleStream> mapper) {
        return new AuxDouble(stream.flatMapToDouble(mapper));
    }
    
    @Override
    public RecordStream distinct() {
        class HeadlessEq {
            final Record record;
            
            HeadlessEq(Record record) {
                this.record = record;
            }
            
            @Override
            public boolean equals(Object o) {
                return Arrays.equals(record.values, ((HeadlessEq) o).record.values);
            }
            
            @Override
            public int hashCode() {
                return Arrays.hashCode(record.values);
            }
        }
        
        return new RecordStream(header, stream.map(HeadlessEq::new).distinct().map(h -> h.record));
    }
    
    @Override
    public RecordStream sorted() {
        return new RecordStream(header, stream.sorted(Utils.HEADLESS_RECORD_COMPARATOR));
    }
    
    @Override
    public RecordStream sorted(Comparator<? super Record> comparator) {
        return new RecordStream(header, stream.sorted(comparator));
    }
    
    @Override
    public RecordStream peek(Consumer<? super Record> action) {
        return new RecordStream(header, stream.peek(action));
    }
    
    @Override
    public RecordStream limit(long maxSize) {
        return new RecordStream(header, stream.limit(maxSize));
    }
    
    @Override
    public RecordStream skip(long n) {
        return new RecordStream(header, stream.skip(n));
    }
    
    @Override
    public void forEach(Consumer<? super Record> action) {
        stream.forEach(action);
    }
    
    @Override
    public void forEachOrdered(Consumer<? super Record> action) {
        stream.forEachOrdered(action);
    }
    
    @Override
    public Object[] toArray() {
        return stream.toArray();
    }
    
    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return stream.toArray(generator);
    }
    
    @Override
    public Record reduce(Record identity, BinaryOperator<Record> accumulator) {
        return stream.reduce(identity, accumulator);
    }
    
    @Override
    public Optional<Record> reduce(BinaryOperator<Record> accumulator) {
        return stream.reduce(accumulator);
    }
    
    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super Record, U> accumulator, BinaryOperator<U> combiner) {
        return stream.reduce(identity, accumulator, combiner);
    }
    
    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super Record> accumulator, BiConsumer<R, R> combiner) {
        return stream.collect(supplier, accumulator, combiner);
    }
    
    @Override
    public <R, A> R collect(Collector<? super Record, A, R> collector) {
        return stream.collect(collector);
    }
    
    @Override
    public Optional<Record> min(Comparator<? super Record> comparator) {
        return stream.min(comparator);
    }
    
    @Override
    public Optional<Record> max(Comparator<? super Record> comparator) {
        return stream.max(comparator);
    }
    
    @Override
    public long count() {
        return stream.count();
    }
    
    @Override
    public boolean anyMatch(Predicate<? super Record> predicate) {
        return stream.anyMatch(predicate);
    }
    
    @Override
    public boolean allMatch(Predicate<? super Record> predicate) {
        return stream.allMatch(predicate);
    }
    
    @Override
    public boolean noneMatch(Predicate<? super Record> predicate) {
        return stream.noneMatch(predicate);
    }
    
    @Override
    public Optional<Record> findFirst() {
        return stream.findFirst();
    }
    
    @Override
    public Optional<Record> findAny() {
        return stream.findAny();
    }
    
    @Override
    public Iterator<Record> iterator() {
        return stream.iterator();
    }
    
    @Override
    public Spliterator<Record> spliterator() {
        return stream.spliterator();
    }
    
    @Override
    public boolean isParallel() {
        return stream.isParallel();
    }
    
    @Override
    public RecordStream sequential() {
        Stream<Record> s = stream.sequential();
        return s == stream ? this : new RecordStream(header, s);
    }
    
    @Override
    public RecordStream parallel() {
        Stream<Record> s = stream.parallel();
        return s == stream ? this : new RecordStream(header, s);
    }
    
    @Override
    public RecordStream unordered() {
        Stream<Record> s = stream.unordered();
        return s == stream ? this : new RecordStream(header, s);
    }
    
    @Override
    public RecordStream onClose(Runnable closeHandler) {
        Stream<Record> s = stream.onClose(closeHandler);
        return s == stream ? this : new RecordStream(header, s);
    }
    
    @Override
    public void close() {
        stream.close();
    }
    
    public static class Aux<T> implements Stream<T> {
        final Stream<T> stream;
        
        Aux(Stream<T> stream) {
            this.stream = stream;
        }
        
        // --- new ---
        
        public RecordStream mapToRecord(Consumer<MapAPI<T>> config) {
            return new MapAPI<T>().accept(this, config);
        }
        
        // --- old ---
    
        @Override
        public Aux<T> filter(Predicate<? super T> predicate) {
            return new Aux<>(stream.filter(predicate));
        }
    
        @Override
        public <R> Aux<R> map(Function<? super T, ? extends R> mapper) {
            return new Aux<>(stream.map(mapper));
        }
    
        @Override
        public AuxInt mapToInt(ToIntFunction<? super T> mapper) {
            return new AuxInt(stream.mapToInt(mapper));
        }
    
        @Override
        public AuxLong mapToLong(ToLongFunction<? super T> mapper) {
            return new AuxLong(stream.mapToLong(mapper));
        }
    
        @Override
        public AuxDouble mapToDouble(ToDoubleFunction<? super T> mapper) {
            return new AuxDouble(stream.mapToDouble(mapper));
        }
    
        @Override
        public <R> Aux<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
            return new Aux<>(stream.flatMap(mapper));
        }
    
        @Override
        public AuxInt flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
            return new AuxInt(stream.flatMapToInt(mapper));
        }
    
        @Override
        public AuxLong flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
            return new AuxLong(stream.flatMapToLong(mapper));
        }
    
        @Override
        public AuxDouble flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
            return new AuxDouble(stream.flatMapToDouble(mapper));
        }
    
        @Override
        public Aux<T> distinct() {
            return new Aux<>(stream.distinct());
        }
    
        @Override
        public Aux<T> sorted() {
            return new Aux<>(stream.sorted());
        }
    
        @Override
        public Aux<T> sorted(Comparator<? super T> comparator) {
            return new Aux<>(stream.sorted(comparator));
        }
    
        @Override
        public Aux<T> peek(Consumer<? super T> action) {
            return new Aux<>(stream.peek(action));
        }
    
        @Override
        public Aux<T> limit(long maxSize) {
            return new Aux<>(stream.limit(maxSize));
        }
    
        @Override
        public Aux<T> skip(long n) {
            return new Aux<>(stream.skip(n));
        }
    
        @Override
        public void forEach(Consumer<? super T> action) {
            stream.forEach(action);
        }
    
        @Override
        public void forEachOrdered(Consumer<? super T> action) {
            stream.forEachOrdered(action);
        }
    
        @Override
        public Object[] toArray() {
            return stream.toArray();
        }
    
        @Override
        public <A> A[] toArray(IntFunction<A[]> generator) {
            return stream.toArray(generator);
        }
    
        @Override
        public T reduce(T identity, BinaryOperator<T> accumulator) {
            return stream.reduce(identity, accumulator);
        }
    
        @Override
        public Optional<T> reduce(BinaryOperator<T> accumulator) {
            return stream.reduce(accumulator);
        }
    
        @Override
        public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
            return stream.reduce(identity, accumulator, combiner);
        }
    
        @Override
        public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
            return stream.collect(supplier, accumulator, combiner);
        }
    
        @Override
        public <R, A> R collect(Collector<? super T, A, R> collector) {
            return stream.collect(collector);
        }
    
        @Override
        public Optional<T> min(Comparator<? super T> comparator) {
            return stream.min(comparator);
        }
    
        @Override
        public Optional<T> max(Comparator<? super T> comparator) {
            return stream.max(comparator);
        }
    
        @Override
        public long count() {
            return stream.count();
        }
    
        @Override
        public boolean anyMatch(Predicate<? super T> predicate) {
            return stream.anyMatch(predicate);
        }
    
        @Override
        public boolean allMatch(Predicate<? super T> predicate) {
            return stream.allMatch(predicate);
        }
    
        @Override
        public boolean noneMatch(Predicate<? super T> predicate) {
            return stream.noneMatch(predicate);
        }
    
        @Override
        public Optional<T> findFirst() {
            return stream.findFirst();
        }
    
        @Override
        public Optional<T> findAny() {
            return stream.findAny();
        }
    
        @Override
        public Iterator<T> iterator() {
            return stream.iterator();
        }
    
        @Override
        public Spliterator<T> spliterator() {
            return stream.spliterator();
        }
    
        @Override
        public boolean isParallel() {
            return stream.isParallel();
        }
    
        @Override
        public Aux<T> sequential() {
            Stream<T> s = stream.sequential();
            return s == stream ? this : new Aux<>(s);
        }
    
        @Override
        public Aux<T> parallel() {
            Stream<T> s = stream.parallel();
            return s == stream ? this : new Aux<>(s);
        }
    
        @Override
        public Aux<T> unordered() {
            Stream<T> s = stream.unordered();
            return s == stream ? this : new Aux<>(s);
        }
    
        @Override
        public Aux<T> onClose(Runnable closeHandler) {
            Stream<T> s = stream.onClose(closeHandler);
            return s == stream ? this : new Aux<>(s);
        }
    
        @Override
        public void close() {
            stream.close();
        }
    }
    
    public static class AuxInt implements IntStream {
        private final IntStream stream;
        
        AuxInt(IntStream stream) {
            this.stream = stream;
        }
    
        @Override
        public AuxInt filter(IntPredicate predicate) {
            return new AuxInt(stream.filter(predicate));
        }
    
        @Override
        public AuxInt map(IntUnaryOperator mapper) {
            return new AuxInt(stream.map(mapper));
        }
    
        @Override
        public <U> Aux<U> mapToObj(IntFunction<? extends U> mapper) {
            return new Aux<>(stream.mapToObj(mapper));
        }
    
        @Override
        public AuxLong mapToLong(IntToLongFunction mapper) {
            return new AuxLong(stream.mapToLong(mapper));
        }
    
        @Override
        public AuxDouble mapToDouble(IntToDoubleFunction mapper) {
            return new AuxDouble(stream.mapToDouble(mapper));
        }
    
        @Override
        public AuxInt flatMap(IntFunction<? extends IntStream> mapper) {
            return new AuxInt(stream.flatMap(mapper));
        }
    
        @Override
        public AuxInt distinct() {
            return new AuxInt(stream.distinct());
        }
    
        @Override
        public AuxInt sorted() {
            return new AuxInt(stream.sorted());
        }
    
        @Override
        public AuxInt peek(IntConsumer action) {
            return new AuxInt(stream.peek(action));
        }
    
        @Override
        public AuxInt limit(long maxSize) {
            return new AuxInt(stream.limit(maxSize));
        }
    
        @Override
        public AuxInt skip(long n) {
            return new AuxInt(stream.skip(n));
        }
    
        @Override
        public void forEach(IntConsumer action) {
            stream.forEach(action);
        }
    
        @Override
        public void forEachOrdered(IntConsumer action) {
            stream.forEachOrdered(action);
        }
    
        @Override
        public int[] toArray() {
            return stream.toArray();
        }
    
        @Override
        public int reduce(int identity, IntBinaryOperator op) {
            return stream.reduce(identity, op);
        }
    
        @Override
        public OptionalInt reduce(IntBinaryOperator op) {
            return stream.reduce(op);
        }
    
        @Override
        public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
            return stream.collect(supplier, accumulator, combiner);
        }
    
        @Override
        public int sum() {
            return stream.sum();
        }
    
        @Override
        public OptionalInt min() {
            return stream.min();
        }
    
        @Override
        public OptionalInt max() {
            return stream.max();
        }
    
        @Override
        public long count() {
            return stream.count();
        }
    
        @Override
        public OptionalDouble average() {
            return stream.average();
        }
    
        @Override
        public IntSummaryStatistics summaryStatistics() {
            return stream.summaryStatistics();
        }
    
        @Override
        public boolean anyMatch(IntPredicate predicate) {
            return stream.anyMatch(predicate);
        }
    
        @Override
        public boolean allMatch(IntPredicate predicate) {
            return stream.allMatch(predicate);
        }
    
        @Override
        public boolean noneMatch(IntPredicate predicate) {
            return stream.noneMatch(predicate);
        }
    
        @Override
        public OptionalInt findFirst() {
            return stream.findFirst();
        }
    
        @Override
        public OptionalInt findAny() {
            return stream.findAny();
        }
    
        @Override
        public AuxLong asLongStream() {
            return new AuxLong(stream.asLongStream());
        }
    
        @Override
        public AuxDouble asDoubleStream() {
            return new AuxDouble(stream.asDoubleStream());
        }
    
        @Override
        public Aux<Integer> boxed() {
            return new Aux<>(stream.boxed());
        }
    
        @Override
        public AuxInt sequential() {
            IntStream s = stream.sequential();
            return s == stream ? this : new AuxInt(s);
        }
    
        @Override
        public AuxInt parallel() {
            IntStream s = stream.parallel();
            return s == stream ? this : new AuxInt(s);
        }
    
        @Override
        public AuxInt unordered() {
            IntStream s = stream.unordered();
            return s == stream ? this : new AuxInt(s);
        }
    
        @Override
        public AuxInt onClose(Runnable closeHandler) {
            IntStream s = stream.onClose(closeHandler);
            return s == stream ? this : new AuxInt(s);
        }
    
        @Override
        public void close() {
            stream.close();
        }
    
        @Override
        public PrimitiveIterator.OfInt iterator() {
            return stream.iterator();
        }
    
        @Override
        public Spliterator.OfInt spliterator() {
            return stream.spliterator();
        }
    
        @Override
        public boolean isParallel() {
            return stream.isParallel();
        }
    }
    
    public static class AuxLong implements LongStream {
        private final LongStream stream;
        
        AuxLong(LongStream stream) {
            this.stream = stream;
        }
    
        @Override
        public AuxLong filter(LongPredicate predicate) {
            return new AuxLong(stream.filter(predicate));
        }
    
        @Override
        public AuxLong map(LongUnaryOperator mapper) {
            return new AuxLong(stream.map(mapper));
        }
    
        @Override
        public <U> Aux<U> mapToObj(LongFunction<? extends U> mapper) {
            return new Aux<>(stream.mapToObj(mapper));
        }
    
        @Override
        public AuxInt mapToInt(LongToIntFunction mapper) {
            return new AuxInt(stream.mapToInt(mapper));
        }
    
        @Override
        public AuxDouble mapToDouble(LongToDoubleFunction mapper) {
            return new AuxDouble(stream.mapToDouble(mapper));
        }
    
        @Override
        public AuxLong flatMap(LongFunction<? extends LongStream> mapper) {
            return new AuxLong(stream.flatMap(mapper));
        }
    
        @Override
        public AuxLong distinct() {
            return new AuxLong(stream.distinct());
        }
    
        @Override
        public AuxLong sorted() {
            return new AuxLong(stream.sorted());
        }
    
        @Override
        public AuxLong peek(LongConsumer action) {
            return new AuxLong(stream.peek(action));
        }
    
        @Override
        public AuxLong limit(long maxSize) {
            return new AuxLong(stream.limit(maxSize));
        }
    
        @Override
        public AuxLong skip(long n) {
            return new AuxLong(stream.skip(n));
        }
    
        @Override
        public void forEach(LongConsumer action) {
            stream.forEach(action);
        }
    
        @Override
        public void forEachOrdered(LongConsumer action) {
            stream.forEachOrdered(action);
        }
    
        @Override
        public long[] toArray() {
            return stream.toArray();
        }
    
        @Override
        public long reduce(long identity, LongBinaryOperator op) {
            return stream.reduce(identity, op);
        }
    
        @Override
        public OptionalLong reduce(LongBinaryOperator op) {
            return stream.reduce(op);
        }
    
        @Override
        public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
            return stream.collect(supplier, accumulator, combiner);
        }
    
        @Override
        public long sum() {
            return stream.sum();
        }
    
        @Override
        public OptionalLong min() {
            return stream.min();
        }
    
        @Override
        public OptionalLong max() {
            return stream.max();
        }
    
        @Override
        public long count() {
            return stream.count();
        }
    
        @Override
        public OptionalDouble average() {
            return stream.average();
        }
    
        @Override
        public LongSummaryStatistics summaryStatistics() {
            return stream.summaryStatistics();
        }
    
        @Override
        public boolean anyMatch(LongPredicate predicate) {
            return stream.anyMatch(predicate);
        }
    
        @Override
        public boolean allMatch(LongPredicate predicate) {
            return stream.allMatch(predicate);
        }
    
        @Override
        public boolean noneMatch(LongPredicate predicate) {
            return stream.noneMatch(predicate);
        }
    
        @Override
        public OptionalLong findFirst() {
            return stream.findFirst();
        }
    
        @Override
        public OptionalLong findAny() {
            return stream.findAny();
        }
    
        @Override
        public AuxDouble asDoubleStream() {
            return new AuxDouble(stream.asDoubleStream());
        }
    
        @Override
        public Aux<Long> boxed() {
            return new Aux<>(stream.boxed());
        }
    
        @Override
        public AuxLong sequential() {
            LongStream s = stream.sequential();
            return s == stream ? this : new AuxLong(s);
        }
    
        @Override
        public AuxLong parallel() {
            LongStream s = stream.parallel();
            return s == stream ? this : new AuxLong(s);
        }
    
        @Override
        public AuxLong unordered() {
            LongStream s = stream.unordered();
            return s == stream ? this : new AuxLong(s);
        }
    
        @Override
        public AuxLong onClose(Runnable closeHandler) {
            LongStream s = stream.onClose(closeHandler);
            return s == stream ? this : new AuxLong(s);
        }
    
        @Override
        public void close() {
            stream.close();
        }
    
        @Override
        public PrimitiveIterator.OfLong iterator() {
            return stream.iterator();
        }
    
        @Override
        public Spliterator.OfLong spliterator() {
            return stream.spliterator();
        }
    
        @Override
        public boolean isParallel() {
            return stream.isParallel();
        }
    }
    
    public static class AuxDouble implements DoubleStream {
        private final DoubleStream stream;
        
        AuxDouble(DoubleStream stream) {
            this.stream = stream;
        }
    
        @Override
        public AuxDouble filter(DoublePredicate predicate) {
            return new AuxDouble(stream.filter(predicate));
        }
    
        @Override
        public AuxDouble map(DoubleUnaryOperator mapper) {
            return new AuxDouble(stream.map(mapper));
        }
    
        @Override
        public <U> Aux<U> mapToObj(DoubleFunction<? extends U> mapper) {
            return new Aux<>(stream.mapToObj(mapper));
        }
    
        @Override
        public AuxInt mapToInt(DoubleToIntFunction mapper) {
            return new AuxInt(stream.mapToInt(mapper));
        }
    
        @Override
        public AuxLong mapToLong(DoubleToLongFunction mapper) {
            return new AuxLong(stream.mapToLong(mapper));
        }
    
        @Override
        public AuxDouble flatMap(DoubleFunction<? extends DoubleStream> mapper) {
            return new AuxDouble(stream.flatMap(mapper));
        }
    
        @Override
        public AuxDouble distinct() {
            return new AuxDouble(stream.distinct());
        }
    
        @Override
        public AuxDouble sorted() {
            return new AuxDouble(stream.sorted());
        }
    
        @Override
        public AuxDouble peek(DoubleConsumer action) {
            return new AuxDouble(stream.peek(action));
        }
    
        @Override
        public AuxDouble limit(long maxSize) {
            return new AuxDouble(stream.limit(maxSize));
        }
    
        @Override
        public AuxDouble skip(long n) {
            return new AuxDouble(stream.skip(n));
        }
    
        @Override
        public void forEach(DoubleConsumer action) {
            stream.forEach(action);
        }
    
        @Override
        public void forEachOrdered(DoubleConsumer action) {
            stream.forEachOrdered(action);
        }
    
        @Override
        public double[] toArray() {
            return stream.toArray();
        }
    
        @Override
        public double reduce(double identity, DoubleBinaryOperator op) {
            return stream.reduce(identity, op);
        }
    
        @Override
        public OptionalDouble reduce(DoubleBinaryOperator op) {
            return stream.reduce(op);
        }
    
        @Override
        public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
            return stream.collect(supplier, accumulator, combiner);
        }
    
        @Override
        public double sum() {
            return stream.sum();
        }
    
        @Override
        public OptionalDouble min() {
            return stream.min();
        }
    
        @Override
        public OptionalDouble max() {
            return stream.max();
        }
    
        @Override
        public long count() {
            return stream.count();
        }
    
        @Override
        public OptionalDouble average() {
            return stream.average();
        }
    
        @Override
        public DoubleSummaryStatistics summaryStatistics() {
            return stream.summaryStatistics();
        }
    
        @Override
        public boolean anyMatch(DoublePredicate predicate) {
            return stream.anyMatch(predicate);
        }
    
        @Override
        public boolean allMatch(DoublePredicate predicate) {
            return stream.allMatch(predicate);
        }
    
        @Override
        public boolean noneMatch(DoublePredicate predicate) {
            return stream.noneMatch(predicate);
        }
    
        @Override
        public OptionalDouble findFirst() {
            return stream.findFirst();
        }
    
        @Override
        public OptionalDouble findAny() {
            return stream.findAny();
        }
    
        @Override
        public Aux<Double> boxed() {
            return new Aux<>(stream.boxed());
        }
    
        @Override
        public AuxDouble sequential() {
            DoubleStream s = stream.sequential();
            return s == stream ? this : new AuxDouble(s);
        }
    
        @Override
        public AuxDouble parallel() {
            DoubleStream s = stream.parallel();
            return s == stream ? this : new AuxDouble(s);
        }
    
        @Override
        public AuxDouble unordered() {
            DoubleStream s = stream.unordered();
            return s == stream ? this : new AuxDouble(s);
        }
    
        @Override
        public AuxDouble onClose(Runnable closeHandler) {
            DoubleStream s = stream.onClose(closeHandler);
            return s == stream ? this : new AuxDouble(s);
        }
    
        @Override
        public void close() {
            stream.close();
        }
    
        @Override
        public PrimitiveIterator.OfDouble iterator() {
            return stream.iterator();
        }
    
        @Override
        public Spliterator.OfDouble spliterator() {
            return stream.spliterator();
        }
    
        @Override
        public boolean isParallel() {
            return stream.isParallel();
        }
    }
}
