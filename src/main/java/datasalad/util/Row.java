package datasalad.util;

import java.util.Arrays;
import java.util.Map;

public class Row {
    private final Map<Column<?>, Integer> indexByColumn;
    private final Comparable<?>[] row;
    
    Row(Map<Column<?>, Integer> indexByColumn, Comparable<?>[] row) {
        this.indexByColumn = indexByColumn;
        this.row = row;
    }
    
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T get(Column<T> column) {
        int index = indexByColumn.get(column);
        return (T) row[index];
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Row))
            return false;
        Row r = (Row) o;
        if (indexByColumn != r.indexByColumn) // Different Dataset
            return false;
        return Arrays.equals(row, r.row);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(row);
    }
}
