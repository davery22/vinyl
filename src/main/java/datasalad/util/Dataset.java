package datasalad.util;

import java.util.stream.Stream;

public class Dataset {
    private final Header header;
    private final Comparable<?>[][] rows;
    
    Dataset(Header header, Comparable<?>[][] rows) {
        this.header = header;
        this.rows = rows;
    }
    
    public Header header() {
        return header;
    }
    
    public DatasetStream stream() {
        return new DatasetStream(header, Stream.of(rows).map(data -> new Row(header, data)));
    }
}
