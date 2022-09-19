package datasalad.util;

import java.util.*;
import java.util.function.Function;

public class MapAPI<T> {
    LinkedHashMap<Column<?>, Function<?, ?>> mapperByColumn = new LinkedHashMap<>();
    
    MapAPI() {} // Prevent default public constructor
    
    public <U extends Comparable<U>> MapAPI<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
        mapperByColumn.put(column, mapper);
        return this;
    }
}
