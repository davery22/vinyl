package vinyl;

import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class RecordSet {
    private final Header header;
    private final Object[][] records;
    
    RecordSet(Header header, Object[][] records) {
        this.header = header;
        this.records = records;
    }
    
    public Header header() {
        return header;
    }
    
    public RecordStream stream() {
        return new RecordStream(header, Stream.of(records).map(values -> new Record(header, values)));
    }
    
    // TODO: Use separate CollectorAPI to permit future persistent indexes?
    public static <T> Collector<T, ?, RecordSet> collector(Consumer<MapAPI<T>> config) {
        return new MapAPI<T>().collector(config);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RecordSet[\n\t[");
        String delimiter = "";
        for (Field<?> field : header.fields) {
            sb.append(delimiter).append(field);
            delimiter = ", ";
        }
        sb.append(']');
        for (Object[] values : records) {
            sb.append(",\n\t[");
            delimiter = "";
            for (Object val : values) {
                sb.append(delimiter).append(val);
                delimiter = ", ";
            }
            sb.append(']');
        }
        return sb.append("\n]").toString();
    }
}
