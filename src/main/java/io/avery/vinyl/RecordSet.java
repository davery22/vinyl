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

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Collector;

/**
 * An ordered collection of {@link Record records} with a common {@link Header header}. Record-sets offer compact
 * storage for such records, and allow for records to be accessed repeatedly, unlike {@link RecordStream record-streams}.
 */
public class RecordSet {
    private final Header header;
    private final Object[][] records;
    
    RecordSet(Header header, Object[][] records) {
        this.header = header;
        this.records = records;
    }
    
    /**
     * Returns the header shared by all records in this record-set.
     *
     * @return the header shared by all records at this record-set
     */
    public Header header() {
        return header;
    }
    
    /**
     * Returns a sequential {@code Stream} with this record-set as its source.
     *
     * @return a sequential {@code Stream} over the records in this record-set
     */
    public RecordStream stream() {
        return new RecordStream(header, Arrays.stream(records).map(values -> new Record(header, values)));
    }
    
    /**
     * Returns the number of records in this record-set.
     *
     * @return the number of records in this record-set
     */
    public int size() {
        return records.length;
    }
    
    /**
     * Returns {@code true} if this record-set contains no records.
     *
     * @return {@code true} if this record-set contains no records
     */
    public boolean isEmpty() {
        return records.length == 0;
    }
    
    /**
     * Returns a {@code Collector} that accumulates the input elements into a new {@code RecordSet}, mapping each input
     * element to a record as configured by the given configurator consumer.
     *
     * @param config a consumer that configures the record fields
     * @return a {@code Collector} which collects all the input elements into a {@code RecordSet}, in encounter order
     * @param <T> the type of the input elements
     */
    public static <T> Collector<T, ?, RecordSet> collector(Consumer<IntoAPI<T>> config) {
        return new IntoAPI<T>().collector(config);
    }
    
    /**
     * Returns {@code true} if and only if the given object is a record-set with a header equal to this set's header,
     * and the object contains records equal to this set's records, in the same order as this set.
     *
     * @param o the object to be compared for equality with this record-set
     * @return {@code true} if the given object is equal to this record-set
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RecordSet))
            return false;
        RecordSet other = (RecordSet) o;
        if (!header.equals(other.header))
            return false;
        int length = records.length;
        if (length != other.records.length)
            return false;
        for (int i = 0; i < length; i++)
            if (!Arrays.equals(records[i], other.records[i]))
                return false;
        return true;
    }
    
    /**
     * Returns the hash code value for this record-set. The hash code of a record-set is derived from the hash code of
     * its header and the hash codes of each of its records.
     *
     * @return the hash code value for this record-set
     */
    @Override
    public int hashCode() {
        int result = header.hashCode();
        for (Object[] arr : records)
            result = 31 * result + Arrays.hashCode(arr);
        return result;
    }
    
    /**
     * Returns a string representation of this record-set. The string representation consists of the characters
     * {@code "RecordSet"}, followed by an array-of-arrays. The first inner array lists the header fields in order,
     * comma-separated. The remaining inner arrays list each record's values in order, comma-separated. The inner arrays
     * are separated by a comma, newline, and tab. Fields and values are converted to strings as by
     * {@link String#valueOf(Object)}. For example:
     *
     * <pre>{@code
     * RecordSet[
     *     [field.a, field.b, field.c],
     *     [value1.a, value1.b, value1.c],
     *     [value2.a, value2.b, value2.c],
     *     [value3.a, value3.b, value3.c]
     * ]
     * }</pre>
     *
     * @return a string representation of this record-set.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RecordSet[\n\t[");
        String delimiter = "";
        for (Field<?> field : header.fields) {
            sb.append(delimiter).append(field);
            delimiter = ", ";
        }
        sb.append(']');
        for (Object[] values : records) {
            sb.append(",\n\t[");
            delimiter = "";
            for (Object val : values) {
                sb.append(delimiter).append(val);
                delimiter = ", ";
            }
            sb.append(']');
        }
        return sb.append("\n]").toString();
    }
}
