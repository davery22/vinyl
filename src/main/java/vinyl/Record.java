package vinyl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class Record {
    final Header header;
    final Object[] values;
    
    Record(Header header, Object[] values) {
        this.header = header;
        this.values = values;
    }
    
    public Header header() {
        return header;
    }
    
    public List<Object> values() {
        return Collections.unmodifiableList(Arrays.asList(values));
    }
    
    @SuppressWarnings("unchecked")
    public <T> T get(Field<T> field) {
        int index = header.indexOf(field);
        if (index == -1)
            throw new NoSuchElementException("Invalid field: " + field);
        return (T) values[index];
    }
    
    @SuppressWarnings("unchecked")
    public <T> T get(FieldPin<T> pin) {
        try {
            if (header.fields[pin.index] == pin.field)
                return (T) values[pin.index];
        } catch (ArrayIndexOutOfBoundsException e) {
            // Fall-through
        }
        throw new NoSuchElementException("Invalid field pin: " + pin);
    }
    
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
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
    
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
}
