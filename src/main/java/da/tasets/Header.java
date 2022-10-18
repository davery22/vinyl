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
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void selectAll(MapAPI<Row> config) {
        for (int i = 0; i < columns.length; i++) {
            Locator locator = new Locator(columns[i], i);
            config.col((Column) columns[i], row -> row.get(locator));
        }
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void selectAllExcept(MapAPI<Row> config, Column<?>... excluded) {
        Set<Column<?>> excludedSet = Set.of(excluded);
        for (int i = 0; i < columns.length; i++) {
            if (excludedSet.contains(columns[i]))
                continue;
            Locator locator = new Locator(columns[i], i);
            config.col((Column) columns[i], row -> row.get(locator));
        }
    }
    
    public void selectAll(SelectAPI config) {
        for (Column<?> column : columns)
            config.col(column);
    }
    
    public void selectAllExcept(SelectAPI config, Column<?>... excluded) {
        Set<Column<?>> excludedSet = Set.of(excluded);
        for (Column<?> column : columns)
            if (!excludedSet.contains(column))
                config.col(column);
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
