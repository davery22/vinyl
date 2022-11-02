package da.tasets;

import java.util.function.Function;
import java.util.function.Supplier;

public class JoinExpr<T> {
    final int side;
    
    JoinExpr(int side) {
        this.side = side;
    }
    
    static class RowExpr<T> extends JoinExpr<T> {
        final Column<T> column; // nullable
        final Function<? super Row, T> mapper;
        
        RowExpr(Column<T> column, int side, Function<? super Row, T> mapper) {
            super(side);
            this.column = column;
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
