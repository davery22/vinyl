package da.tasets;

public final class FieldPin<T> {
    final Field<T> field;
    final int index;
    
    public FieldPin(Field<T> field, int index) {
        this.field = field;
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
    public String toString() {
        return field.toString() + " @ " + index;
    }
}
