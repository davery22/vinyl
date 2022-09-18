package datasalad.util;

public class Column<T extends Comparable<T>> {
    private final String name;
    
    public Column(String name) {
        this.name = name;
    }
    
    @Override
    public String toString() {
        return name;
    }
}
