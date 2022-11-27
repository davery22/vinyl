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
import java.util.stream.*;

/**
 * A stream of {@link Record records} with a common {@link Header header}. In addition to the usual stream operations,
 * a {@code RecordStream} supports several relational-style operations that yield a new {@code RecordStream} with a
 * new header and transformed records. A record-stream's header can always be safely accessed via {@link #header()},
 * even after the stream has been operated on.
 */
public class RecordStream implements Stream<Record> {
    final Header header;
    final Stream<Record> stream;
    
    RecordStream(Header header, Stream<Record> stream) {
        this.header = header;
        this.stream = stream;
    }
    
    // --- new ---
    
    /**
     * Returns the header shared by all records at this stage in the stream pipeline.
     *
     * @return the header shared by all records at this stage in the stream pipeline
     */
    public Header header() {
        return header;
    }
    
    /**
     * Concatenates the input record-streams to form a new record-stream, similarly to
     * {@link Stream#concat Stream.concat}, as long as the input stream headers are equal. If the headers are not equal,
     * throws {@link IllegalArgumentException}.
     *
     * @param a the first stream
     * @param b the second stream
     * @return the concatenation of the two input streams
     * @throws IllegalArgumentException if the input stream headers are not equal
     * @see Stream#concat(Stream, Stream)
     */
    public static RecordStream concat(RecordStream a, RecordStream b) {
        if (!a.header.equals(b.header))
            throw new IllegalArgumentException("Header mismatch");
        return new RecordStream(a.header, Stream.concat(a.stream, b.stream));
    }
    
    /**
     * Converts the given stream to an auxiliary object stream, or returns the given stream if it is already an
     * auxiliary object stream.
     *
     * @param stream the stream to convert
     * @return an auxiliary object stream
     * @param <T> the type of the stream elements
     */
    @SuppressWarnings("unchecked")
    public static <T> Aux<T> aux(Stream<T> stream) {
        if (stream instanceof RecordStream)
            return (Aux<T>) ((RecordStream) stream).aux();
        if (stream instanceof Aux)
            return (Aux<T>) stream;
        return new Aux<>(stream);
    }
    
    /**
     * Converts the given stream to an auxiliary int stream, or returns the given stream if it is already an auxiliary
     * int stream.
     *
     * @param stream the stream to covert
     * @return an auxiliary int stream
     */
    public static AuxInt aux(IntStream stream) {
        if (stream instanceof AuxInt)
            return (AuxInt) stream;
        return new AuxInt(stream);
    }
    
    /**
     * Converts the given stream to an auxiliary long stream, or returns the given stream if it is already an auxiliary
     * long stream.
     *
     * @param stream the stream to covert
     * @return an auxiliary long stream
     */
    public static AuxLong aux(LongStream stream) {
        if (stream instanceof AuxLong)
            return (AuxLong) stream;
        return new AuxLong(stream);
    }
    
    /**
     * Converts the given stream to an auxiliary double stream, or returns the given stream if it is already an
     * auxiliary double stream.
     *
     * @param stream the stream to covert
     * @return an auxiliary double stream
     */
    public static AuxDouble aux(DoubleStream stream) {
        if (stream instanceof AuxDouble)
            return (AuxDouble) stream;
        return new AuxDouble(stream);
    }
    
    /**
     * Accumulates the records of this stream into a {@code RecordSet}. The records in the set will be in this stream's
     * encounter order, if one exists. The record-set will have the same header as this stream.
     *
     * <p>This is a terminal operation.
     *
     * @return a record-set containing the stream records
     */
    public RecordSet toRecordSet() {
        return new RecordSet(header, stream.map(record -> record.values).toArray(Object[][]::new));
    }
    
    /**
     * Converts this stream to an auxiliary object stream consisting of the records of this stream.
     *
     * @return an auxiliary object stream
     */
    public Aux<Record> aux() {
        return new Aux<>(stream);
    }
    
    /**
     * Returns a stream that maps each record in this stream to a new record, as configured by the given configurator
     * consumer. The returned stream will have a new header, shared by the new records.
     *
     * <p>This is a stateless intermediate operation, by default. If {@link SelectAPI.Window windows} are configured,
     * this becomes a stateful intermediate operation.
     *
     * <p>If {@link SelectAPI.Window windows} are configured, subsequent changes to the sequential/parallel execution
     * mode of the returned stream will <em>not</em> propagate to the input stream.
     *
     * @param config a consumer that configures the output fields
     * @return a stream that maps each record in this stream to a new record
     */
    public RecordStream select(Consumer<SelectAPI> config) {
        return new SelectAPI(this).accept(config);
    }
    
    /**
     * Returns a stream that aggregates the records in this stream, possibly within grouped partitions. Each partition
     * will result in one record in the returned stream. The grouping and aggregation are carried out as configured by
     * the given configurator consumer. The returned stream will have a new header, shared by the new records.
     *
     * <p>This is a stateful intermediate operation.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will <em>not</em>
     * propagate to the input stream.
     *
     * @param config a consumer that configures the aggregate operation
     * @return a stream that aggregates the records in this stream
     */
    public RecordStream aggregate(Consumer<AggregateAPI> config) {
        return new AggregateAPI(this).accept(config);
    }
    
    /**
     * Returns a stream that performs an inner join between this (left) stream and the given right stream. The join
     * streams the left-side, and for each left-side record, searches the right-side for matching records such that the
     * (left, right) pair of records satisfy the configured join condition. When a match is found, a resulting record is
     * created as configured and emitted downstream. The returned stream will have a new header, shared by the new
     * records.
     *
     * <p>For an inner join, if a given left-side record does not match any right-side records, or vice versa, nothing
     * is emitted downstream.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will propagate to the left
     * stream, but not the right stream. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @param selectConfig a consumer that configures the output fields
     * @return the new stream
     */
    public RecordStream join(RecordStream right,
                             Function<JoinAPI.On, JoinPred> onConfig,
                             Consumer<JoinAPI.Select> selectConfig) {
        return new JoinAPI(JoinAPI.JoinType.INNER, this, right).accept(onConfig, selectConfig);
    }
    
    /**
     * Returns a stream that performs a left outer join between this (left) stream and the given right stream. The join
     * streams the left-side, and for each left-side record, searches the right-side for matching records such that the
     * (left, right) pair of records satisfy the configured join condition. When a match is found, a resulting record is
     * created as configured and emitted downstream. The returned stream will have a new header, shared by the new
     * records.
     *
     * <p>For a left join, if a given left-side record does not match any right-side records, it is paired with a
     * {@link Record#isNil() nil} right-side record, and a resulting record is created as configured and emitted
     * downstream. If a right-side record does not match any left-side records, nothing is emitted downstream.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will propagate to the left
     * stream, but not the right stream. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @param selectConfig a consumer that configures the output fields
     * @return the new stream
     */
    public RecordStream leftJoin(RecordStream right,
                                 Function<JoinAPI.On, JoinPred> onConfig,
                                 Consumer<JoinAPI.Select> selectConfig) {
        return new JoinAPI(JoinAPI.JoinType.LEFT, this, right).accept(onConfig, selectConfig);
    }
    
    /**
     * Returns a stream that performs a right outer join between this (left) stream and the given right stream. The join
     * streams the left-side, and for each left-side record, searches the right-side for matching records such that the
     * (left, right) pair of records satisfy the configured join condition. When a match is found, a resulting record is
     * created as configured and emitted downstream. The returned stream will have a new header, shared by the new
     * records.
     *
     * <p>For a right join, if a given left-side record does not match any right-side records, nothing is emitted
     * downstream. If a right-side record does not match any left-side records, it is paired with a
     * {@link Record#isNil() nil} left-side record, and a resulting record is created as configured and emitted
     * downstream.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will <em>not</em>
     * propagate to the input streams. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @param selectConfig a consumer that configures the output fields
     * @return the new stream
     */
    public RecordStream rightJoin(RecordStream right,
                                  Function<JoinAPI.On, JoinPred> onConfig,
                                  Consumer<JoinAPI.Select> selectConfig) {
        return new JoinAPI(JoinAPI.JoinType.RIGHT, this, right).accept(onConfig, selectConfig);
    }
    
    /**
     * Returns a stream that performs a full outer join between this (left) stream and the given right stream. The join
     * streams the left-side, and for each left-side record, searches the right-side for matching records such that the
     * (left, right) pair of records satisfy the configured join condition. When a match is found, a resulting record is
     * created as configured and emitted downstream. The returned stream will have a new header, shared by the new
     * records.
     *
     * <p>For a full join, if a given left-side record does not match any right-side records, or vice versa, the
     * unmatched record is paired with a {@link Record#isNil() nil} opposite-side record, and a resulting record is
     * created as configured and emitted downstream.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will <em>not</em>
     * propagate to the input streams. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @param selectConfig a consumer that configures the output fields
     * @return the new stream
     */
    public RecordStream fullJoin(RecordStream right,
                                 Function<JoinAPI.On, JoinPred> onConfig,
                                 Consumer<JoinAPI.Select> selectConfig) {
        return new JoinAPI(JoinAPI.JoinType.FULL, this, right).accept(onConfig, selectConfig);
    }
    
    /**
     * Returns a stream consisting of the records of this (left) stream that match with any records of the given right
     * stream. This "join" streams the left-side, and for each left-side record, searches the right-side for matching
     * records such that the (left, right) pair of records satisfy the configured join condition.
     *
     * <p>This operation is also known as a left semi join.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will propagate to the left
     * stream, but not the right stream. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @return the new stream
     */
    public RecordStream leftAnyMatch(RecordStream right,
                                     Function<JoinAPI.On, JoinPred> onConfig) {
        return new JoinAPI(JoinAPI.JoinType.LEFT_ANY_MATCH, this, right).semiAccept(onConfig);
    }
    
    /**
     * Returns a stream consisting of the records of this (left) stream that do not match with any records of the given
     * right stream. This "join" streams the left-side, and for each left-side record, searches the right-side for
     * matching records such that the (left, right) pair of records satisfy the configured join condition.
     *
     * <p>This operation is also known as a left anti (semi) join.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will propagate to the left
     * stream, but not the right stream. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @return the new stream
     */
    public RecordStream leftNoneMatch(RecordStream right,
                                      Function<JoinAPI.On, JoinPred> onConfig) {
        return new JoinAPI(JoinAPI.JoinType.LEFT_NONE_MATCH, this, right).semiAccept(onConfig);
    }
    
    /**
     * Returns a stream consisting of the records of this (left) stream that match with all records of the given right
     * stream. This "join" streams the left-side, and for each left-side record, searches the right-side for matching
     * records such that the (left, right) pair of records satisfy the configured join condition.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will propagate to the left
     * stream, but not the right stream. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @return the new stream
     */
    public RecordStream leftAllMatch(RecordStream right,
                                     Function<JoinAPI.On, JoinPred> onConfig) {
        return new JoinAPI(JoinAPI.JoinType.LEFT_ALL_MATCH, this, right).semiAccept(onConfig);
    }
    
    /**
     * Returns a stream consisting of the records of this (left) stream that do not match with all records of the given
     * right stream. This "join" streams the left-side, and for each left-side record, searches the right-side for
     * matching records such that the (left, right) pair of records satisfy the configured join condition.
     *
     * <p>This is a stateless intermediate operation on the left stream, and a stateful intermediate operation on the
     * right stream.
     *
     * <p>Subsequent changes to the sequential/parallel execution mode of the returned stream will propagate to the left
     * stream, but not the right stream. When the returned stream is closed, the close handlers for both input streams
     * are invoked.
     *
     * @param right the right-side stream
     * @param onConfig a function that configures the join condition
     * @return the new stream
     */
    public RecordStream leftNotAllMatch(RecordStream right,
                                        Function<JoinAPI.On, JoinPred> onConfig) {
        return new JoinAPI(JoinAPI.JoinType.LEFT_NOT_ALL_MATCH, this, right).semiAccept(onConfig);
    }
    
    public <R> RecordStream crossApply(Function<? super Record, ? extends Stream<? extends R>> mapper,
                                       Consumer<ApplyAPI<R>> config) {
        return new ApplyAPI<R>(ApplyAPI.ApplyType.CROSS, this).accept(mapper, config);
    }
    
    public <R> RecordStream outerApply(Function<? super Record, ? extends Stream<? extends R>> mapper,
                                       Consumer<ApplyAPI<R>> config) {
        return new ApplyAPI<R>(ApplyAPI.ApplyType.OUTER, this).accept(mapper, config);
    }
    
    // --- old ---
    
    @Override
    public RecordStream takeWhile(Predicate<? super Record> predicate) {
        return new RecordStream(header, stream.takeWhile(predicate));
    }
    
    @Override
    public RecordStream dropWhile(Predicate<? super Record> predicate) {
        return new RecordStream(header, stream.dropWhile(predicate));
    }
    
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
        return new RecordStream(header, stream.distinct());
    }
    
    /**
     * Only safe when Records have the same Header and all Fields are Comparable.
     */
    static final Comparator<Record> HEADLESS_RECORD_COMPARATOR = (a, b) -> {
        for (int i = 0; i < a.values.length; i++) {
            int v = Utils.DEFAULT_COMPARATOR.compare(a.values[i], b.values[i]);
            if (v != 0)
                return v;
        }
        return 0;
    };
    
    /**
     * Returns a stream consisting of the records of this stream, sorted according to the natural order (with nulls
     * first/lowest) of each header field's value type, in turn. If any header field's value type is not
     * {@code Comparable}, a {@code java.lang.ClassCastException} may be thrown when the terminal operation is executed.
     *
     * <p>For ordered streams, the sort is stable. For unordered streams, no stability guarantees are made.
     *
     * <p>This is a stateful intermediate operation.
     *
     * @return the new stream
     */
    @Override
    public RecordStream sorted() {
        return new RecordStream(header, stream.sorted(HEADLESS_RECORD_COMPARATOR));
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
    
    /**
     * An auxiliary stream of objects that may readily be converted to or from a {@link RecordStream record-stream}.
     *
     * @param <T> the type of the stream elements
     */
    public static class Aux<T> implements Stream<T> {
        final Stream<T> stream;
        
        Aux(Stream<T> stream) {
            this.stream = stream;
        }
        
        // --- new ---
    
        /**
         * Returns a stream that maps each element in this stream to a record, as configured by the given configurator
         * consumer. The returned record-stream will have a header shared by all its records.
         *
         * @param config a consumer that configures the record fields
         * @return a stream that maps each element in this stream to a record
         */
        public RecordStream mapToRecord(Consumer<IntoAPI<T>> config) {
            return new IntoAPI<T>().accept(this, config);
        }
        
        public <R> RecordStream flatMapToRecord(Function<? super T, ? extends Stream<? extends R>> mapper,
                                                Consumer<BiIntoAPI<T, R>> config) {
            return new BiIntoAPI<T, R>().accept(this, mapper, config);
        }
        
        // --- old ---
        
        @Override
        public Aux<T> takeWhile(Predicate<? super T> predicate) {
            return new Aux<>(stream.takeWhile(predicate));
        }
    
        @Override
        public Aux<T> dropWhile(Predicate<? super T> predicate) {
            return new Aux<>(stream.dropWhile(predicate));
        }
    
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
    
    /**
     * An auxiliary stream of ints that may readily be converted to or from a {@link RecordStream record-stream}.
     */
    public static class AuxInt implements IntStream {
        private final IntStream stream;
        
        AuxInt(IntStream stream) {
            this.stream = stream;
        }
    
        @Override
        public AuxInt takeWhile(IntPredicate predicate) {
            return new AuxInt(stream.takeWhile(predicate));
        }
    
        @Override
        public AuxInt dropWhile(IntPredicate predicate) {
            return new AuxInt(stream.dropWhile(predicate));
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
    
    /**
     * An auxiliary stream of longs that may readily be converted to or from a {@link RecordStream record-stream}.
     */
    public static class AuxLong implements LongStream {
        private final LongStream stream;
        
        AuxLong(LongStream stream) {
            this.stream = stream;
        }
    
        @Override
        public AuxLong takeWhile(LongPredicate predicate) {
            return new AuxLong(stream.takeWhile(predicate));
        }
    
        @Override
        public AuxLong dropWhile(LongPredicate predicate) {
            return new AuxLong(stream.dropWhile(predicate));
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
    
    /**
     * An auxiliary stream of doubles that may readily be converted to or from a {@link RecordStream record-stream}.
     */
    public static class AuxDouble implements DoubleStream {
        private final DoubleStream stream;
        
        AuxDouble(DoubleStream stream) {
            this.stream = stream;
        }
    
        @Override
        public AuxDouble takeWhile(DoublePredicate predicate) {
            return new AuxDouble(stream.takeWhile(predicate));
        }
    
        @Override
        public AuxDouble dropWhile(DoublePredicate predicate) {
            return new AuxDouble(stream.dropWhile(predicate));
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
