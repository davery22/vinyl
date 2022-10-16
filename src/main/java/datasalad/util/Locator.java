package datasalad.util;

public final class Locator<T> {
    final Column<T> column;
    final int index;
    
    public Locator(Column<T> column, int index) {
        this.column = column;
        this.index = index;
    }
    
    public T get(Row row) {
        return row.get(this);
    }
    
    @Override
    public String toString() {
        return column.toString() + " @ " + index;
    }
}
