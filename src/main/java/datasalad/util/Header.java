package datasalad.util;

import java.util.*;

class Header {
    final Map<Column<?>, Integer> indexByColumn;
    final List<Column<?>> columns;
    
    Header(Map<Column<?>, Integer> indexByColumn) {
        Column<?>[] cols = new Column[indexByColumn.size()];
        indexByColumn.forEach((col, i) -> cols[i] = col);
        this.columns = Arrays.asList(cols);
        this.indexByColumn = indexByColumn;
    }
    
    public int indexOf(Column<?> column) {
        return indexByColumn.get(column);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Header))
            return false;
        return columns.equals(((Header) o).columns);
    }
    
    @Override
    public int hashCode() {
        return columns.hashCode();
    }
    
    @Override
    public String toString() {
        return "Header" + columns;
    }
}
