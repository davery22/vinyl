package datasalad.util;

public final class Locator<T extends Comparable<? super T>> {
    final Column<T> column;
    final int index;
    
    public Locator(Column<T> column, int index) {
        this.column = column;
        this.index = index;
    }
    
    @Override
    public String toString() {
        return column.toString();
    }
}
