package da.tasets;

public final class Field<T> {
    private final String name;
    
    public Field(String name) {
        this.name = name;
    }
    
    public T get(Record record) {
        return record.get(this);
    }
    
    @Override
    public String toString() {
        return name;
    }
}
