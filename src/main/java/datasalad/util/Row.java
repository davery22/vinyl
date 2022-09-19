package datasalad.util;

import java.util.Arrays;

public class Row {
    private final Header header;
    final Comparable<?>[] data;
    
    Row(Header header, Comparable<?>[] data) {
        this.header = header;
        this.data = data;
    }
    
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T get(Column<T> column) {
        int index = header.indexByColumn.get(column);
        return (T) data[index];
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
}
