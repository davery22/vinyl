package vinyl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        return (T) values[index];
    }
    
    @SuppressWarnings("unchecked")
    public <T> T get(FieldPin<T> pin) {
        // Throws AIOOBE
        if (header.fields[pin.index] != pin.field)
            throw new IllegalArgumentException("Invalid field pin: " + pin);
        return (T) values[pin.index];
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
