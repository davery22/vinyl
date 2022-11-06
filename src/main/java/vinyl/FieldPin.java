package vinyl;

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An object that associates a field with a header index, for optimized field value lookup. In other words, a
 * {@code FieldPin} "pins" a field to its index in some header. Record field value lookups using a field-pin are able to
 * skip computing the field index in the record header, by instead just checking that the header contains the pin's
 * field at the pin's index.
 *
 * @param <T> the value type of the field
 */
public final class FieldPin<T> {
    final Field<T> field;
    final int index;
    
    /**
     * Creates a new {@code FieldPin} from the given field and index.
     *
     * @param field the field
     * @param index the index
     */
    public FieldPin(Field<T> field, int index) {
        this.field = Objects.requireNonNull(field);
        this.index = index;
    }
    
    /**
     * Returns the value associated with this pin's field in the given record, or throws {@link NoSuchElementException}
     * if the record's header does not contain this pin's field at this pin's index.
     *
     * <p>This method is equivalent to {@code record.get(this)}, and is provided mainly to enable more concise method
     * references ({@code pin::get}) in certain situations.
     *
     * @param record the record whose associated value for this pin's field is to be returned
     * @return the value associated with this pin's field
     * @throws NoSuchElementException if the record's header does not contain this pin's field at this pin's index
     * @see Record#get(FieldPin)
     */
    public T get(Record record) {
        return record.get(this);
    }
    
    /**
     * Returns this pin's field.
     *
     * @return this pin's field
     */
    public Field<T> field() {
        return field;
    }
    
    /**
     * Returns this pin's index.
     *
     * @return this pin's index
     */
    public int index() {
        return index;
    }
    
    /**
     * Returns {@code true} if and only if the given object is a field-pin with the same field and index as this field
     * pin.
     *
     * @param o the object to be compared for equality with this field-pin
     * @return {@code true} if the given object is equal to this field-pin
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FieldPin))
            return false;
        FieldPin<?> other = (FieldPin<?>) o;
        return field == other.field && index == other.index;
    }
    
    /**
     * Returns the hash code for this field-pin. The hash code of a field-pin is derived from the hash code of its field
     * and its index.
     *
     * @return the hash code for this field-pin
     */
    @Override
    public int hashCode() {
        return 5331 * (field.hashCode() + index);
    }
    
    /**
     * Returns a string representation of this field-pin. The string representation of a field-pin is:
     *
     * <pre>{@code
     * field + " @ " + index
     * }</pre>
     *
     * @return a string representation of this field-pin
     */
    @Override
    public String toString() {
        return field + " @ " + index;
    }
}
