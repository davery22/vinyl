package datasalad.util;

public final class Column<T> {
    private final String name;
    
    public Column(String name) {
        this.name = name;
    }
    
    @Override
    public String toString() {
        return name;
    }
}
