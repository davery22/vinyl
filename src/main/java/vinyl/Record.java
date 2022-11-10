package vinyl;

import java.util.*;

/**
 * A shallowly immutable carrier for a fixed set of values, determined by the record {@link Header header}. Values are
 * stored in ordered correspondence with the header fields. However, record values are primarily accessed via a
 * type-safe accessor that accepts a {@link Field field} or a {@link FieldPin field-pin}.
 *
 * <p>Records can be viewed as simple java objects, whose "fields" are fixed at instance-creation time, rather than
 * class-declaration time. This trades some unsafety (a user may attempt to access fields that a record does not
 * contain) for the ability to dynamically introduce new "shapes" of records - a pre-requisite for many relational
 * operations.
 *
 * <p>Records have a natural definition of {@code equals()}, {@code hashCode()}, and {@code toString()}, based on the
 * record header and values.
 */
public class Record {
    final Header header;
    final Object[] values;
    
    Record(Header header, Object[] values) {
        this.header = header;
        this.values = values;
    }
    
    /**
     * Returns the record {@link Header header}. The header {@link Header#fields() fields} are in ordered correspondence
     * with the record field {@link #values() values}.
     *
     * @return the record header
     */
    public Header header() {
        return header;
    }
    
    /**
     * Returns an unmodifiable view of the record field values. The values are in ordered correspondence with the record
     * {@link #header() header} fields.
     *
     * @return the record field values
     */
    public List<Object> values() {
        return Collections.unmodifiableList(Arrays.asList(values));
    }
    
    /**
     * Returns the value associated with the given field in this record, or throws {@link NoSuchElementException} if
     * this record's header does not contain the field.
     *
     * @param field the field whose associated value is to be returned
     * @return the value associated with the given field
     * @throws NoSuchElementException if this record's header does not contain the field
     * @param <T> the type of the value
     */
    @SuppressWarnings("unchecked")
    public <T> T get(Field<T> field) {
        int index = header.indexOf(field);
        if (index == -1)
            throw new NoSuchElementException("Invalid field: " + field);
        return (T) values[index];
    }
    
    /**
     * Returns the value associated with the given pin's field in this record, or throws {@link NoSuchElementException}
     * if this record's header does not contain the pin's field at the pin's index.
     *
     * <p>This is an optimized lookup that takes advantage of the field index being known in advance.
     *
     * @param pin the pin whose field's associated value is to be returned
     * @return the value associated with the given pin's field
     * @throws NoSuchElementException if this record's header does not contain the pin's field at the pin's index
     * @param <T> the type of the value
     */
    @SuppressWarnings("unchecked")
    public <T> T get(FieldPin<T> pin) {
        try {
            if (header.fields[pin.index] == pin.field)
                return (T) values[pin.index];
        } catch (ArrayIndexOutOfBoundsException e) {
            // Fall-through
        }
        throw new NoSuchElementException("Invalid field-pin: " + pin);
    }
    
    /**
     * Returns {@code true} if this is a "nil" record that was synthesized to pair with an unmatched record in an outer
     * (left/right/full) join. If {@code true}, all of the record field values are {@code null}.
     *
     * @return {@code true} if this is a "nil" record
     * @see RecordStream#leftJoin
     * @see RecordStream#rightJoin
     * @see RecordStream#fullJoin
     */
    public boolean isNil() {
        return false;
    }
    
    /**
     * Returns {@code true} if and only if the given object is a record with a header and values equal to this record's
     * header and values, respectively.
     *
     * @param o the object to be compared for equality with this record
     * @return {@code true} if the given object is equal to this record
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Record))
            return false;
        Record other = (Record) o;
        if (!header.equals(other.header))
            return false;
        return Arrays.equals(values, other.values);
    }
    
    /**
     * Returns the hash code value for this record. The hash code of a record is derived from the hash code of each of
     * its values.
     *
     * @return the hash code value for this record
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
    
    /**
     * Returns a string representation of this record. The string representation consists of a list of field-value
     * associations, in the same order as the header fields, enclosed in the braces of {@code "Record{}"}. Adjacent
     * associations are separated by the characters {@code ", "} (comma and space). Each field-value association is
     * rendered as the field followed by an equals sign ({@code "="}) followed by the associated value. Fields and
     * values are converted to strings as by {@link String#valueOf(Object)}.
     *
     * @return a string representation of this record
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Record{");
        String delimiter = "";
        for (int i = 0; i < values.length; i++) {
            sb.append(delimiter).append(header.fields[i]).append('=').append(values[i]);
            delimiter = ", ";
        }
        return sb.append('}').toString();
    }
    
    /**
     * Special record type used ephemerally by window / analytic functions to associate input records with output
     * records.
     */
    static class LinkedRecord extends Record {
        final Object[] out;
        
        LinkedRecord(Record in, Object[] out) {
            super(in.header, in.values);
            this.out = out;
        }
    }
    
    /**
     * Special record type used ephemerally by right/full joins to keep track of unmatched records on the right. During
     * the join, records that are matched are flagged. Remaining unmatched records are emitted after the join.
     */
    static class FlaggedRecord extends Record {
        // This field may be modified by un-synchronized concurrent writers during a parallel join. This is safe,
        // because all writers try to set it to the same value (true), and the only reader waits for all writers to
        // terminate (so all writes are visible to it).
        boolean isMatched = false;
        final Record record;
        
        FlaggedRecord(Record record) {
            super(record.header, record.values);
            this.record = record;
        }
    }
    
    /**
     * Special record type produced by outer (left/right/full) joins to indicate a record of null values, that was
     * synthesized to pair with an unmatched record. Note that the lifetime of nil records may be extended if user code
     * stores them into fields, but nil records will never be first-class records in a record-stream.
     */
    static class NilRecord extends Record {
        NilRecord(Header header, Object[] values) {
            super(header, values);
        }
        
        @Override
        public boolean isNil() {
            return true;
        }
    }
}
