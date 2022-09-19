package datasalad.util;

import java.util.function.*;

public class JoinAPI {
    JoinAPI() {} // Prevent default public constructor
    
    public JoinAPI on(Consumer<On> config) {
        return this;
    }
    
    public JoinAPI andSelect(Consumer<Select> config) {
        return this;
    }
    
    public static class On {
        On() {} // Prevent default public constructor
    
        public <T extends Comparable<T>> JoinExpr<T> left(Column<T> column) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> right(Column<T> column) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> left(Function<? super Row, T> mapper) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> right(Function<? super Row, T> mapper) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> val(Supplier<T> val) {
            return null;
        }
        
        public JoinPred not(JoinPred predicate) {
            return null;
        }
        
        public JoinPred all(JoinPred... predicates) {
            return null;
        }
        
        public JoinPred any(JoinPred... predicates) {
            return null;
        }
        
        public JoinPred lcheck(Predicate<? super Row> predicate) {
            return null;
        }
        
        public JoinPred rcheck(Predicate<? super Row> predicate) {
            return null;
        }
        
        public JoinPred check(BiPredicate<? super Row, ? super Row> predicate) {
            return null;
        }
    }
    
    public static class Select {
        Select() {} // Prevent default public constructor
        
        public Select lall() {
            return this;
        }
        
        public Select rall() {
            return this;
        }
        
        public Select lallExcept(Column<?>... columns) {
            return this;
        }
        
        public Select rallExcept(Column<?>... columns) {
            return this;
        }
        
        public Select lcol(Column<?> column) {
            return this;
        }
        
        public Select rcol(Column<?> column) {
            return this;
        }
        
        public <T extends Comparable<T>> Select col(Column<T> column, BiFunction<? super Row, ? super Row, ? extends T> mapper) {
            return this;
        }
        
        public <T> Select cols(BiFunction<? super Row, ? super Row, ? extends T> mapper, Cols<T> config) {
            return this;
        }
        
        public static class Cols<T> {
            Cols() {} // Prevent default public constructor
            
            public <U extends Comparable<U>> Cols<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
                return this;
            }
        }
    }
}
