package da.tasets;

import java.util.function.Function;
import java.util.function.Supplier;

public class JoinExpr<T> {
    final int side;
    
    JoinExpr(int side) {
        this.side = side;
    }
    
    T get(Row left, Row right) {
        throw new UnsupportedOperationException();
    }
    
    static class LCol<T> extends JoinExpr<T> {
        final Locator<T> locator;
        
        LCol(Locator<T> locator) {
            super(JoinAPI.LEFT);
            this.locator = locator;
        }
        
        @Override
        T get(Row left, Row right) {
            return left.get(locator);
        }
    }
    
    static class RCol<T> extends JoinExpr<T> {
        final Locator<T> locator;
        
        RCol(Locator<T> locator) {
            super(JoinAPI.RIGHT);
            this.locator = locator;
        }
        
        @Override
        T get(Row left, Row right) {
            return right.get(locator);
        }
    }
    
    static class LExpr<T> extends JoinExpr<T> {
        final Function<? super Row, T> mapper;
        
        LExpr(Function<? super Row, T> mapper) {
            super(JoinAPI.LEFT);
            this.mapper = mapper;
        }
        
        @Override
        T get(Row left, Row right) {
            return mapper.apply(left);
        }
    }
    
    static class RExpr<T> extends JoinExpr<T> {
        final Function<? super Row, T> mapper;
        
        RExpr(Function<? super Row, T> mapper) {
            super(JoinAPI.RIGHT);
            this.mapper = mapper;
        }
        
        @Override
        T get(Row left, Row right) {
            return mapper.apply(right);
        }
    }
    
    static class Expr<T> extends JoinExpr<T> {
        final Supplier<T> supplier;
        
        Expr(Supplier<T> supplier) {
            super(JoinAPI.NONE);
            this.supplier = supplier;
        }
        
        // `Expr` should be evaluated once, not for each row.
    }
}
