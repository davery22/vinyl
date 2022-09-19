package datasalad.util;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class Header {
    final Map<Column<?>, Integer> indexByColumn;
    private final List<Column<?>> columns;
    
    Header(List<Column<?>> columns) {
        this.columns = columns;
        this.indexByColumn = new IdentityHashMap<>();
        for (int i = 0; i < columns.size(); i++)
            indexByColumn.put(columns.get(i), i);
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
