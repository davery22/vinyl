package da.tasets;

import java.util.function.Function;
import java.util.function.Supplier;

public class JoinExpr<T> {
    final int side;
    
    JoinExpr(int side) {
        this.side = side;
    }
    
    static class RecordExpr<T> extends JoinExpr<T> {
        final Field<T> field; // nullable
        final Function<? super Record, T> mapper;
        
        RecordExpr(Field<T> field, int side, Function<? super Record, T> mapper) {
            super(side);
            this.field = field;
            this.mapper = mapper;
        }
    }
    
    static class Expr<T> extends JoinExpr<T> {
        final Supplier<T> supplier;
        
        Expr(Supplier<T> supplier) {
            super(JoinAPI.NONE);
            this.supplier = supplier;
        }
    }
}
