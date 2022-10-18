package da.tasets;

public final class Column<T> {
    private final String name;
    
    public Column(String name) {
        this.name = name;
    }
    
    public T get(Row row) {
        return row.get(this);
    }
    
    @Override
    public String toString() {
        return name;
    }
}
