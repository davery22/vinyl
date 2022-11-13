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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

/**
 * Factory methods that create various useful analytic functions, of the form expected by {@link SelectAPI.Window}.
 */
public class Analytics {
    private Analytics() {} // Prevent instantiation
    
    /**
     * Converts an aggregate function (modeled by a {@code Collector}) to an analytic function that emits the single
     * result of the aggregate function.
     *
     * @param collector the aggregate function
     * @return an analytic function that emits the single result of the aggregate function
     * @param <T> the aggregate result type
     */
    public static <T> BiConsumer<List<Record>, Consumer<T>> fromAgg(Collector<? super Record, ?, T> collector) {
        Objects.requireNonNull(collector);
        return (list, sink) -> sink.accept(list.stream().collect(collector));
    }
   
    /**
     * Returns an analytic function that emits a number for each record in the partition, by starting with the given
     * first number and incrementing by one for each record.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to some
     * comparator.
     *
     * <p>This analytic function behaves similar to the {@code ROW_NUMBER} function in SQL.
     *
     * @param firstNumber the starting number
     * @return an analytic function that emits a number for each record in the partition
     */
    public static BiConsumer<List<Record>, Consumer<Long>> rowNumber(long firstNumber) {
        return (list, sink) -> {
            for (long i = 0; i < list.size(); i++)
                sink.accept(i + firstNumber);
        };
    }
    
    /**
     * Returns an analytic function that emits a rank for each record in the partition, by starting with the given first
     * rank and assigning the same rank to consecutively tied records, incrementing the rank by the number of ties when
     * the tie is broken. Consecutive records are considered tied if they compare equal using the given comparator.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to the
     * given comparator.
     *
     * <p>This analytic function behaves similar to the {@code RANK} function in SQL.
     *
     * @param firstRank the starting rank
     * @param comparator a comparator used to identify ties between consecutive records
     * @return an analytic function that emits a rank for each record in the partition
     */
    public static BiConsumer<List<Record>, Consumer<Long>> rank(long firstRank, Comparator<? super Record> comparator) {
        Objects.requireNonNull(comparator);
        return (list, sink) -> {
            Record prev = list.get(0);
            long rank = firstRank;
            sink.accept(rank);
            for (int i = 1; i < list.size(); i++)
                if (comparator.compare(prev, prev = list.get(i)) == 0)
                    sink.accept(rank);
                else
                    sink.accept(rank = i + firstRank);
        };
    }
    
    /**
     * Returns an analytic function that emits a rank for each record in the partition, by starting with the given first
     * rank and assigning the same rank to consecutive tied records, incrementing the rank by one when the tie is
     * broken. Consecutive records are considered tied if they compare equal using the given comparator.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to the
     * given comparator.
     *
     * <p>This analytic function behaves similar to the {@code DENSE_RANK} function in SQL.
     *
     * @param firstRank the starting rank
     * @param comparator a comparator used to identify ties between consecutive records
     * @return an analytic function that emits a rank for each record in the partition
     */
    public static BiConsumer<List<Record>, Consumer<Long>> denseRank(long firstRank, Comparator<? super Record> comparator) {
        Objects.requireNonNull(comparator);
        return (list, sink) -> {
            Record prev = list.get(0);
            long rank = firstRank;
            sink.accept(rank);
            for (int i = 1; i < list.size(); i++)
                if (comparator.compare(prev, prev = list.get(i)) == 0)
                    sink.accept(rank);
                else
                    sink.accept(++rank);
        };
    }
    
    /**
     * Returns an analytic function that distributes records in the partition into the given {@code n} number of groups.
     *
     * <p>Group numbers are emitted by starting with the given first group number, emitting the same group number up to
     * the group size, and then incrementing the group number by one. If the number of records in the partition is
     * divisible by {@code n}, groups will be the same size. If the number of records in the partition is not divisible
     * by {@code n}, the remainder will be distributed by giving earlier groups one extra member, until there is no more
     * remainder.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to some
     * comparator.
     *
     * <p>This analytic function behaves similar to the {@code NTILE} function in SQL.
     *
     * @param firstGroup the starting group number
     * @param n the number of groups
     * @return an analytic function that distributes records in the partition into the given {@code n} number of groups
     */
    public static BiConsumer<List<Record>, Consumer<Long>> nTile(long firstGroup, int n) {
        if (n < 1)
            throw new IllegalArgumentException("n must be positive; was: " + n);
        return (list, sink) -> {
            int size = list.size();
            int groupSize = size / n;
            int remainder = size % n;
            int currSize = 0;
            long group = firstGroup;
            for (Record ignored : list)
                if (currSize < groupSize) { // Still room in this group
                    ++currSize;
                    sink.accept(group);
                }
                else if (remainder > 0) { // Extra member in this group
                    --remainder;
                    currSize = 0;
                    sink.accept(group++);
                }
                else { // Start next group
                    currSize = 1;
                    sink.accept(++group);
                }
        };
    }
    
    /**
     * Returns an analytic function that links each record in the partition to a value derived from another record at a
     * given fixed offset away. The offset is added to the record index to obtain the index of the record to link to,
     * and the given function is applied to that offset record to obtain a value to emit. If the offset index lies out
     * of bounds, the given default value is emitted. If the offset is {@code 0}, the record links to itself.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to some
     * comparator.
     *
     * <p>This analytic function behaves similar to the {@code LEAD} and {@code LAG} functions in SQL.
     *
     * @param mapper a function to be applied to each offset record to obtain a value
     * @param offset the fixed offset from each record index
     * @param defaultValue the value to emit when the offset index lies out of bounds
     * @return an analytic function that links each record in the partition to a value from another record at a given
     * fixed offset away
     * @param <T> the linked value type
     */
    public static <T> BiConsumer<List<Record>, Consumer<T>> link(Function<? super Record, ? extends T> mapper,
                                                                 int offset,
                                                                 T defaultValue) {
        Objects.requireNonNull(mapper);
        return (list, sink) -> {
            int size = list.size();
            for (int i = 0; i < size; i++) {
                int j = i + offset;
                if (j < 0 || j >= size)
                    sink.accept(defaultValue);
                else
                    sink.accept(mapper.apply(list.get(j)));
            }
        };
    }
    
    /**
     * Returns an analytic function that emits the cumulative distribution value of each record in the partition. This
     * value is defined as the number of records preceding a record, plus consecutively succeeding tied records, all
     * divided by the total number of records. Consecutive records are considered tied if they compare equal using the
     * given comparator. The cumulative distribution value is always greater than {@code 0} and less than or equal to
     * {@code 1}.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to the
     * given comparator. This makes the cumulative distribution value of a record equal to the number of records with
     * values less than or equal to the value of that record, divided by the total number of records.
     *
     * <p>This analytic function behaves similar to the {@code CUME_DIST} function in SQL.
     *
     * @param comparator a comparator used to identify ties between consecutive records
     * @return an analytic function that emits the cumulative distribution value of each record in the partition
     */
    public static BiConsumer<List<Record>, Consumer<Double>> cumeDist(Comparator<? super Record> comparator) {
        Objects.requireNonNull(comparator);
        return (list, sink) -> {
            Record prev = list.get(0);
            int size = list.size();
            int ties = 1;
            for (int i = 1; i < size; i++)
                if (comparator.compare(prev, prev = list.get(i)) == 0)
                    ties++;
                else {
                    double percentile = (double) i / size; // Divide each time to minimize FP error and match SQL behavior
                    for (; ties > 0; ties--)
                        sink.accept(percentile);
                    ties = 1;
                }
            for (; ties > 0; ties--)
                sink.accept(1d);
        };
    }
    
    /**
     * Returns an analytic function that emits the percent rank value of each record in the partition. This value is
     * defined as the number of records preceding a record, minus consecutively preceding tied records, all divided by
     * one less than the total number of records. Consecutive records are considered tied if they compare equal using
     * the given comparator. The percent rank value is always greater than or equal to {@code 0} and less than or equal
     * to {@code 1}. The first record in any partition always has a percent rank of {@code 0}.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to the
     * given comparator. This makes the percent rank value of a record equal to the number of records with values less
     * than the value of that record, divided by one less than the total number of records.
     *
     * <p>This analytic function behaves similar to the {@code PERCENT_RANK} function in SQL.
     *
     * @param comparator a comparator used to identify ties between consecutive records
     * @return an analytic function that emits the cumulative distribution value of each record in the partition
     */
    public static BiConsumer<List<Record>, Consumer<Double>> percentRank(Comparator<? super Record> comparator) {
        Objects.requireNonNull(comparator);
        return (list, sink) -> {
            double percentile = 0d;
            Record prev = list.get(0);
            int size = list.size();
            int ties = 1;
            for (int i = 1; i < size; i++)
                if (comparator.compare(prev, prev = list.get(i)) == 0)
                    ties++;
                else {
                    for (; ties > 0; ties--)
                        sink.accept(percentile);
                    ties = 1;
                    percentile = (double) i / (size-1); // Divide each time to minimize FP error and match SQL behavior
                }
            for (; ties > 0; ties--)
                sink.accept(percentile);
        };
    }
    
    /**
     * Returns an analytic function that emits one value for the partition, derived from the record that is positioned
     * closest to the given percentile's index. More specifically, the percentile is multiplied by the last index in the
     * partition, yielding an exact percentile index. Then a value is emitted by applying the given function to the
     * record whose index is closest to the percentile index.
     *
     * <p>The given percentile must be greater than or equal to {@code 0} and less than or equal to {@code 1}, otherwise
     * an {@link IllegalArgumentException} is thrown.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to a
     * comparator that compares records based on the value that would be returned by applying the given function to the
     * record.
     *
     * <p>This analytic function behaves similar to the {@code PERCENTILE_DISC} function in SQL.
     *
     * @param percentile the percentile
     * @param mapper a function to be applied to the record closest to the percentile index
     * @return an analytic function that emits one value for the partition, based on a discontinuous interpretation of
     * the percentile
     * @param <T> the value type
     * @throws IllegalArgumentException if the given percentile is less than {@code 0} or greater than {@code 1}
     */
    public static <T> BiConsumer<List<Record>, Consumer<T>> percentileDisc(double percentile,
                                                                           Function<? super Record, ? extends T> mapper) {
        Objects.requireNonNull(mapper);
        if (percentile < 0d || percentile > 1d)
            throw new IllegalArgumentException("percentile must range between 0.0 and 1.0; was: " + percentile);
        return (list, sink) -> {
            double exactIndex = percentile * (list.size()-1);
            int floor = (int) exactIndex;
            double fractionalIndex = exactIndex - floor;
            int index = fractionalIndex <= 0.5 ? floor : floor+1; // <= 0.5 to match SQL behavior
            sink.accept(mapper.apply(list.get(index)));
        };
    }
    
    /**
     * Returns an analytic function that emits one value for the partition, derived by interpolation from the values of
     * the records that are positioned closest on either side of the given percentile's index. More specifically, the
     * percentile is multiplied by the last index in the partition, yielding an exact percentile index. Then a value is
     * emitted by applying the given function to the two records on either side of the percentile index, and
     * interpolating a value between them based on the relative distance from the percentile index to each record index.
     * If the percentile index is a mathematical integer, then interpolation can be skipped, as the percentile index
     * will exactly match a single record index.
     *
     * <p>The given percentile must be greater than or equal to {@code 0} and less than or equal to {@code 1}, otherwise
     * an {@link IllegalArgumentException} is thrown.
     *
     * <p>This analytic function generally expects input records in the partition to be pre-sorted according to a
     * comparator that compares records based on the value that would be returned by applying the given function to the
     * record.
     *
     * <p>This analytic function behaves similar to the {@code PERCENTILE_CONT} function in SQL.
     *
     * @param percentile the percentile
     * @param mapper a function to be applied to the records closest to the percentile index
     * @return an analytic function that emits one value for the partition, based on a continuous interpretation of the
     * percentile
     * @throws IllegalArgumentException if the given percentile is less than {@code 0} or greater than {@code 1}
     */
    public static BiConsumer<List<Record>, Consumer<Double>> percentileCont(double percentile,
                                                                            Function<? super Record, ? extends Number> mapper) {
        Objects.requireNonNull(mapper);
        if (percentile < 0d || percentile > 1d)
            throw new IllegalArgumentException("percentile must range between 0.0 and 1.0; was: " + percentile);
        return (list, sink) -> {
            double exactIndex = percentile * (list.size()-1);
            int floor = (int) exactIndex;
            double fractionalIndex = exactIndex - floor;
            double floorVal = mapper.apply(list.get(floor)).doubleValue();
            if (fractionalIndex == 0d) {
                sink.accept(floorVal);
                return;
            }
            int ceil = (int) (exactIndex + 1);
            double ceilVal = mapper.apply(list.get(ceil)).doubleValue();
            double val = floorVal + (ceilVal - floorVal) * fractionalIndex;
            sink.accept(val);
        };
    }
}
