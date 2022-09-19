package datasalad.util;

import java.util.function.Consumer;
import java.util.function.Function;

public class SelectAPI {
    SelectAPI() {} // Prevent default public constructor
    
    public SelectAPI all() {
        return this;
    }
    
    public SelectAPI allExcept(Column<?>... columns) {
        return this;
    }
    
    public SelectAPI col(Column<?> column) {
        return this;
    }
    
    public <T extends Comparable<T>> SelectAPI col(Column<T> column, Function<? super Row, ? extends T> mapper) {
        return this;
    }
    
    public <T> SelectAPI cols(Function<? super Row, ? extends T> mapper, Consumer<Cols<T>> config) {
        return this;
    }
    
    public static class Cols<T> {
        Cols() {} // Prevent default public constructor
        
        public <U extends Comparable<U>> Cols<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
            return this;
        }
    }
}
