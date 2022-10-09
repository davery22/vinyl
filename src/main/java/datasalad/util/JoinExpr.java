package datasalad.util;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinExpr<T extends Comparable<? super T>> {
    final int side;
    
    JoinExpr(int side) {
        this.side = side;
    }
    
    public JoinPred eq(JoinExpr<?> right) {
        return new JoinPred.Binary(JoinPred.Binary.Op.EQ, this, right);
    }
    
    public JoinPred neq(JoinExpr<?> right) {
        return new JoinPred.Binary(JoinPred.Binary.Op.NEQ, this, right);
    }
    
    public JoinPred gt(JoinExpr<T> right) {
        return new JoinPred.Binary(JoinPred.Binary.Op.GT, this, right);
    }
    
    public JoinPred gte(JoinExpr<T> right) {
        return new JoinPred.Binary(JoinPred.Binary.Op.GTE, this, right);
    }
    
    public JoinPred lt(JoinExpr<T> right) {
        return new JoinPred.Binary(JoinPred.Binary.Op.LT, this, right);
    }
    
    public JoinPred lte(JoinExpr<T> right) {
        return new JoinPred.Binary(JoinPred.Binary.Op.LTE, this, right);
    }
    
    public JoinPred in(JoinExpr<?>... exprs) {
        // TODO: Use Set?
        return new JoinPred.AnyAll(true, Stream.of(exprs).map(this::eq).collect(Collectors.toList()));
    }
    
    public JoinPred between(JoinExpr<T> start, JoinExpr<T> end) {
        return new JoinPred.AnyAll(false, List.of(this.gte(start), this.lte(end)));
    }
    
    // TODO
//    public JoinPred like(Supplier<String> pattern) {
//        throw new UnsupportedOperationException();
//    }
    
    T get(Row left, Row right) {
        throw new UnsupportedOperationException();
    }
    
    static class LCol<T extends Comparable<? super T>> extends JoinExpr<T> {
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
    
    static class RCol<T extends Comparable<? super T>> extends JoinExpr<T> {
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
    
    static class LExpr<T extends Comparable<? super T>> extends JoinExpr<T> {
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
    
    static class RExpr<T extends Comparable<? super T>> extends JoinExpr<T> {
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
    
    static class Expr<T extends Comparable<? super T>> extends JoinExpr<T> {
        final Supplier<T> supplier;
        
        Expr(Supplier<T> supplier) {
            super(JoinAPI.NONE);
            this.supplier = supplier;
        }
        
        // `Expr` should be evaluated once, not for each row.
    }
}
