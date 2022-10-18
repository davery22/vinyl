package da.tasets;

import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class Dataset {
    private final Header header;
    private final Object[][] rows;
    
    Dataset(Header header, Object[][] rows) {
        this.header = header;
        this.rows = rows;
    }
    
    public Header header() {
        return header;
    }
    
    public DatasetStream stream() {
        return new DatasetStream(header, Stream.of(rows).map(data -> new Row(header, data)));
    }
    
    // TODO: Use separate CollectorAPI to permit future persistent indexes?
    public static <T> Collector<T, ?, Dataset> collector(Consumer<MapAPI<T>> config) {
        return new MapAPI<T>().collector(config);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Dataset[\n\t[");
        String delimiter = "";
        for (Column<?> column : header.columns) {
            sb.append(delimiter).append(column);
            delimiter = ", ";
        }
        sb.append(']');
        for (Object[] row : rows) {
            sb.append(",\n\t[");
            delimiter = "";
            for (Object val : row) {
                sb.append(delimiter).append(val);
                delimiter = ", ";
            }
            sb.append(']');
        }
        return sb.append("\n]").toString();
    }
}
