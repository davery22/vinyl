package datasalad.util;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JoinPred {
    JoinPred() {} // Prevent default public constructor
    
    JoinPred negate() { throw new UnsupportedOperationException(); }
    
    static class Not extends JoinPred {
        final JoinPred pred;
        
        Not(JoinPred pred) {
            this.pred = pred;
        }
        
        JoinPred negate() {
            return pred;
        }
    }
    
    static class Binary extends JoinPred {
        enum Op { EQ, NEQ, GT, GTE, LT, LTE }
    
        final Op op;
        final JoinExpr<?> left;
        final JoinExpr<?> right;
        
        Binary(Op op, JoinExpr<?> left, JoinExpr<?> right) {
            this.op = op;
            this.left = left;
            this.right = right;
        }
        
        Binary negate() {
            switch (op) {
                case EQ: return new Binary(Op.NEQ, left, right);
                case NEQ: return new Binary(Op.EQ, left, right);
                case GT: return new Binary(Op.LTE, left, right);
                case GTE: return new Binary(Op.LT, left, right);
                case LT: return new Binary(Op.GTE, left, right);
                case LTE: return new Binary(Op.GT, left, right);
                default: throw new AssertionError();
            }
        }
    }
    
    static class AnyAll extends JoinPred {
        final boolean isAny;
        final List<JoinPred> preds;
        
        AnyAll(boolean isAny, List<JoinPred> preds) {
            this.isAny = isAny;
            this.preds = preds;
        }
        
        AnyAll negate() {
            return new AnyAll(!isAny, preds.stream().map(JoinPred::negate).collect(Collectors.toList()));
        }
    }
    
    static class SideMatch extends JoinPred {
        final boolean isLeft;
        final Predicate<? super Row> predicate;
        
        SideMatch(boolean isLeft, Predicate<? super Row> predicate) {
            this.isLeft = isLeft;
            this.predicate = predicate;
        }
        
        SideMatch negate() {
            return new SideMatch(isLeft, predicate.negate());
        }
    }
    
    static class Match extends JoinPred {
        final BiPredicate<? super Row, ? super Row> predicate;
        
        Match(BiPredicate<? super Row, ? super Row> predicate) {
            this.predicate = predicate;
        }
        
        Match negate() {
            return new Match(predicate.negate());
        }
    }
}
