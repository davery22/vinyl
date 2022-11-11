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

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An identifier used to access a typed value in a {@link Record record}. Fields are organized into record
 * {@link Header headers}, that may be shared by many records.
 *
 * <p>Fields use default object {@code equals()} and {@code hashCode()}. That is, two fields are only equal if they are
 * the same object.
 *
 * @param <T> the value type of the field
 */
public final class Field<T> {
    private final String name;
    
    /**
     * Creates a new field with the given name, used by the field's string representation.
     *
     * @param name the field name, used by the field's string representation.
     */
    public Field(String name) {
        this.name = Objects.requireNonNull(name);
    }
    
    /**
     * Returns the value associated with this field in the given record, or throws {@link NoSuchElementException} if the
     * record's header does not contain this field.
     *
     * <p>This method is equivalent to {@code record.get(this)}, and is provided mainly to enable more concise method
     * references ({@code field::get}) in certain situations.
     *
     * @param record the record whose associated value for this field is to be returned
     * @return the value associated with this field
     * @throws NoSuchElementException if the record's header does not contain this field
     * @see Record#get(Field)
     */
    public T get(Record record) {
        return record.get(this);
    }
    
    /**
     * Returns the field name
     *
     * @return the field name
     */
    @Override
    public String toString() {
        return name;
    }
}
