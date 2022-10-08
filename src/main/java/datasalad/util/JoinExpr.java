package datasalad.util;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinExpr<T extends Comparable<? super T>> {
    JoinExpr() {} // Prevent default public constructor
    
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
    
    static class Col<T extends Comparable<? super T>> extends JoinExpr<T> {
        final boolean isLeft;
        final Column<T> column;
        
        Col(boolean isLeft, Column<T> column) {
            this.isLeft = isLeft;
            this.column = column;
        }
    }
    
    static class SideExpr<T extends Comparable<? super T>> extends JoinExpr<T> {
        final boolean isLeft;
        final Function<? super Row, T> mapper;
        
        SideExpr(boolean isLeft, Function<? super Row, T> mapper) {
            this.isLeft = isLeft;
            this.mapper = mapper;
        }
    }
    
    static class Expr<T extends Comparable<? super T>> extends JoinExpr<T> {
        final Supplier<T> supplier;
        
        Expr(Supplier<T> supplier) {
            this.supplier = supplier;
        }
    }
}
