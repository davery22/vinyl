package datasalad.util;

import datasalad.util.stream.DatasetStream;

import java.util.Map;

public class Dataset {
    private final Map<Column<?>, Integer> indexByColumn;
    private final Comparable<?>[][] rows;
    
    private Dataset(Map<Column<?>, Integer> indexByColumn, Comparable<?>[][] rows) {
        this.indexByColumn = indexByColumn;
        this.rows = rows;
    }
    
    public DatasetStream stream() {
        throw new UnsupportedOperationException();
    }
}
