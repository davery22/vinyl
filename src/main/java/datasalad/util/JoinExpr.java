package datasalad.util;

import java.util.function.Supplier;

public class JoinExpr<T extends Comparable<T>> {
    JoinExpr() {} // Prevent default public constructor
    
    public JoinPred eq(JoinExpr<?> right) {
        return null;
    }
    
    public JoinPred neq(JoinExpr<?> right) {
        return null;
    }
    
    public JoinPred in(JoinExpr<?>... exprs) {
        return null;
    }
    
    public JoinPred gt(JoinExpr<T> right) {
        return null;
    }
    
    public JoinPred gte(JoinExpr<T> right) {
        return null;
    }
    
    public JoinPred lt(JoinExpr<T> right) {
        return null;
    }
    
    public JoinPred lte(JoinExpr<T> right) {
        return null;
    }
    
    public JoinPred between(JoinExpr<T> start, JoinExpr<T> end) {
        return null;
    }
    
    public JoinPred like(Supplier<String> pattern) {
        return null;
    }
}
