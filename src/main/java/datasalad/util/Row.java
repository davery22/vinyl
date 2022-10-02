package datasalad.util;

import java.util.Arrays;
import java.util.Comparator;

public class Row {
    /**
     * Only safe when both rows are known to use the same types / have the same header.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static final Comparator<Row> HEADLESS_COMPARATOR = (r1, r2) ->
        Arrays.compare((Comparable[]) r1.data, (Comparable[]) r2.data);
    
    private final Header header;
    final Comparable<?>[] data;
    
    Row(Header header, Comparable<?>[] data) {
        this.header = header;
        this.data = data;
    }
    
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T get(Column<T> column) {
        int index = header.indexOf(column);
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
