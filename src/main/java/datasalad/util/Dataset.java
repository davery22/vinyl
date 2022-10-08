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
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Dataset[\n");
        String delimiter = "\t[";
        for (Column<?> column : header.columns) {
            sb.append(delimiter).append(column);
            delimiter = ", ";
        }
        sb.append(']');
        for (Comparable<?>[] row : rows) {
            delimiter = ",\n\t[";
            for (Comparable<?> val : row) {
                sb.append(delimiter).append(val);
                delimiter = ", ";
            }
            sb.append("]");
        }
        return sb.append("\n]").toString();
    }
}
