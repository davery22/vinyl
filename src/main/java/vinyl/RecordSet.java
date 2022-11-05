package vinyl;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Collector;

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
        return new RecordStream(header, Arrays.stream(records).map(values -> new Record(header, values)));
    }
    
    // TODO: Use separate CollectorAPI to permit future persistent indexes?
    public static <T> Collector<T, ?, RecordSet> collector(Consumer<MapAPI<T>> config) {
        return new MapAPI<T>().collector(config);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RecordSet))
            return false;
        RecordSet other = (RecordSet) o;
        if (!header.equals(other.header))
            return false;
        int length = records.length;
        if (length != other.records.length)
            return false;
        for (int i = 0; i < length; i++)
            if (!Arrays.equals(records[i], other.records[i]))
                return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        int result = header.hashCode();
        for (Object[] arr : records)
            result = 31 * result + Arrays.hashCode(arr);
        return result;
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
