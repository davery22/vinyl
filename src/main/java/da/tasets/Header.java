package da.tasets;

import java.util.*;

class Header {
    final Map<Column<?>, Integer> indexByColumn;
    final Column<?>[] columns;
    
    Header(Map<Column<?>, Integer> indexByColumn) {
        this.indexByColumn = indexByColumn;
        this.columns = new Column[indexByColumn.size()];
        indexByColumn.forEach((col, i) -> columns[i] = col);
    }
    
    public int indexOf(Column<?> column) {
        return Objects.requireNonNull(indexByColumn.get(column), () -> "Unknown column: " + column);
    }
    
    public <T> Locator<T> locator(Column<T> column) {
        return new Locator<>(column, indexOf(column));
    }
    
    public List<Column<?>> columns() {
        return Collections.unmodifiableList(Arrays.asList(columns));
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Header))
            return false;
        return Arrays.equals(columns, ((Header) o).columns);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(columns);
    }
    
    @Override
    public String toString() {
        return "Header" + Arrays.toString(columns);
    }
}
