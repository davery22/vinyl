package vinyl;

import java.util.*;

public class Header {
    final Map<Field<?>, Integer> indexByField;
    final Field<?>[] fields;
    
    Header(Map<Field<?>, Integer> indexByField) {
        this.indexByField = indexByField;
        this.fields = new Field[indexByField.size()];
        indexByField.forEach((field, i) -> fields[i] = field);
    }
    
    public int indexOf(Field<?> field) {
        return Objects.requireNonNull(indexByField.get(field), () -> "Unknown field: " + field);
    }
    
    public <T> FieldPin<T> pin(Field<T> field) {
        return new FieldPin<>(field, indexOf(field));
    }
    
    public List<Field<?>> fields() {
        return Collections.unmodifiableList(Arrays.asList(fields));
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Header))
            return false;
        return Arrays.equals(fields, ((Header) o).fields);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }
    
    @Override
    public String toString() {
        return "Header" + Arrays.toString(fields);
    }
}
