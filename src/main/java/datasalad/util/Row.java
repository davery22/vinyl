package datasalad.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Row {
    private final Header header;
    final Object[] data;
    
    Row(Header header, Object[] data) {
        this.header = header;
        this.data = data;
    }
    
    public Header header() {
        return header;
    }
    
    public List<Object> data() {
        return Collections.unmodifiableList(Arrays.asList(data));
    }
    
    @SuppressWarnings("unchecked")
    public <T> T get(Column<T> column) {
        int index = header.indexOf(column);
        return (T) data[index];
    }
    
    @SuppressWarnings("unchecked")
    public <T> T get(Locator<T> locator) {
        // Throws AIOOB
        if (header.columns[locator.index] != locator.column)
            throw new IllegalArgumentException("Invalid locator: " + locator);
        return (T) data[locator.index];
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Row))
            return false;
        Row other = (Row) o;
        if (!header.equals(other.header))
            return false;
        return Arrays.equals(data, other.data);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Row[");
        String delimiter = "";
        for (int i = 0; i < data.length; i++) {
            sb.append(delimiter).append(header.columns[i]).append('=').append(data[i]);
            delimiter = ", ";
        }
        return sb.append(']').toString();
    }
}
