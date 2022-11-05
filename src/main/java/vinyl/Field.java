package vinyl;

import java.util.Objects;

public final class Field<T> {
    private final String name;
    
    public Field(String name) {
        this.name = Objects.requireNonNull(name);
    }
    
    public T get(Record record) {
        return record.get(this);
    }
    
    @Override
    public String toString() {
        return name;
    }
}
