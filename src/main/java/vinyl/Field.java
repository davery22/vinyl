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
