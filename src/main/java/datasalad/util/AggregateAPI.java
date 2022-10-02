package datasalad.util;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class AggregateAPI {
    AggregateAPI() {} // Prevent default public constructor
    
    public AggregateAPI tempKey(Column<?> column) {
        return this;
    }
    
    public <T extends Comparable<T>> AggregateAPI tempKey(Function<? super Row, ? extends T> mapper) {
        return this;
    }
    
    public AggregateAPI key(Column<?> column) {
        return this;
    }
    
    public <T extends Comparable<T>> AggregateAPI key(Column<T> column, Function<? super Row, ? extends T> mapper) {
        return this;
    }
    
    public <T> AggregateAPI keys(Function<? super Row, ? extends T> mapper, Consumer<Keys<T>> config) {
        return this;
    }
    
    public <T extends Comparable<T>, A> AggregateAPI agg(Column<T> column, Collector<? super Row, A, ? extends T> collector) {
        return this;
    }
    
    public <T, A> AggregateAPI aggs(Collector<? super Row, A, ? extends T> collector, Consumer<Aggs<T>> config) {
        return this;
    }
    
    public static class Keys<T> {
        Keys() {} // Prevent default public constructor
        
        public <U extends Comparable<U>> Keys<T> key(Column<U> column, Function<? super T, ? extends U> mapper) {
            return this;
        }
    }
    
    public static class Aggs<T> {
        Aggs() {} // Prevent default public constructor
        
        public <U extends Comparable<U>> Aggs<T> agg(Column<U> column, Function<? super T, ? extends U> mapper) {
            return this;
        }
    }
}
