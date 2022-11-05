package vinyl;

import java.util.Objects;

public final class FieldPin<T> {
    final Field<T> field;
    final int index;
    
    public FieldPin(Field<T> field, int index) {
        this.field = Objects.requireNonNull(field);
        this.index = index;
    }
    
    public T get(Record record) {
        return record.get(this);
    }
    
    public Field<T> field() {
        return field;
    }
    
    public int index() {
        return index;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FieldPin))
            return false;
        FieldPin<?> other = (FieldPin<?>) o;
        return field == other.field && index == other.index;
    }
    
    @Override
    public int hashCode() {
        return 5331 * (field.hashCode() + index);
    }
    
    @Override
    public String toString() {
        return field + " @ " + index;
    }
}
