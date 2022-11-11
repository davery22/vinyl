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

import java.util.*;

/**
 * An ordered, immutable collection of {@link Field fields} belonging to a single {@link Record record}, a
 * {@link RecordSet record-set}, or a {@link RecordStream record-stream}. Header fields are distinct from each other.
 */
public class Header {
    final Map<Field<?>, Integer> indexByField;
    final Field<?>[] fields;
    
    Header(Map<Field<?>, Integer> indexByField) {
        this.indexByField = indexByField;
        this.fields = new Field[indexByField.size()];
        indexByField.forEach((field, i) -> fields[i] = field);
    }
    
    /**
     * Returns the index of the given field in this header, or {@code -1} if this header does not contain the field.
     *
     * @param field the field to search for
     * @return the index of the given field in this header, or {@code -1} if this header does not contain the field
     */
    public int indexOf(Field<?> field) {
        Objects.requireNonNull(field);
        Integer index = indexByField.get(field);
        return index != null ? index : -1;
    }
    
    /**
     * Returns a {@link FieldPin pin} associating the given field with its index in this header, or throws
     * {@link NoSuchElementException} if this header does not contain the field.
     *
     * @param field the field to be pinned
     * @return a pin for the field
     * @param <T> the value type of the field
     * @throws NoSuchElementException if this header does not contain the field
     */
    public <T> FieldPin<T> pin(Field<T> field) {
        Objects.requireNonNull(field);
        Integer index = indexByField.get(field);
        if (index == null)
            throw new NoSuchElementException("Invalid field: " + field);
        return new FieldPin<>(field, index);
    }
    
    /**
     * Returns an unmodifiable view of the header fields.
     *
     * @return the header fields
     */
    public List<Field<?>> fields() {
        return Collections.unmodifiableList(Arrays.asList(fields));
    }
    
    /**
     * Returns {@code true} if and only if the given object is a header containing the same fields in the same order as
     * this header.
     *
     * @param o the object to be compared for equality with this header
     * @return {@code true} if the given object is equal to this header
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Header))
            return false;
        return Arrays.equals(fields, ((Header) o).fields);
    }
    
    /**
     * Returns the hash code value for this header. The hash code of a header is derived from the hash codes of each of
     * its fields.
     *
     * @return the hash code value for this header
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }
    
    /**
     * Returns a string representation of this header. The string representation consists of the characters
     * {@code "Header"}, followed by the string representation of the header {@link #fields()}.
     *
     * @return a string representation of this header
     */
    @Override
    public String toString() {
        return "Header" + Arrays.toString(fields);
    }
}
