package datasalad.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class JoinPred {
    /**
     * Natural order, nulls first/lowest.
     */
    private static final Comparator<Comparable<Object>> DEFAULT_ORDER =
        (a, b) -> a == null ? (b == null ? 0 : -1) : a.compareTo(b);
    
    JoinPred() {} // Prevent default public constructor
    
    BiPredicate<? super Row, ? super Row> toPredicate() {
        throw new UnsupportedOperationException();
    }
    
    void accept(Visitor visitor) {
        throw new UnsupportedOperationException();
    }
    
    static class Not extends JoinPred {
        final JoinPred pred;
        
        Not(JoinPred pred) {
            this.pred = pred;
        }
        
        void accept(Visitor visitor) {
            visitor.visit(this);
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
    
        void accept(Visitor visitor) {
            visitor.visit(this);
        }
    
        int side() {
            return left.side | right.side;
        }
    
        @Override
        BiPredicate<? super Row, ? super Row> toPredicate() {
            switch (op) {
                case EQ:  return (lt, rt) ->  Objects.equals(left.get(lt, rt), right.get(lt, rt));
                case NEQ: return (lt, rt) -> !Objects.equals(left.get(lt, rt), right.get(lt, rt));
                case GT:  return (lt, rt) -> DEFAULT_ORDER.compare(cast(left.get(lt, rt)), cast(right.get(lt, rt))) >  0;
                case GTE: return (lt, rt) -> DEFAULT_ORDER.compare(cast(left.get(lt, rt)), cast(right.get(lt, rt))) >= 0;
                case LT:  return (lt, rt) -> DEFAULT_ORDER.compare(cast(left.get(lt, rt)), cast(right.get(lt, rt))) <  0;
                case LTE: return (lt, rt) -> DEFAULT_ORDER.compare(cast(left.get(lt, rt)), cast(right.get(lt, rt))) <= 0;
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
    
        void accept(Visitor visitor) {
            visitor.visit(this);
        }
    
        @Override
        BiPredicate<? super Row, ? super Row> toPredicate() {
            @SuppressWarnings("unchecked")
            BiPredicate<? super Row, ? super Row>[] predicates = preds.stream().map(JoinPred::toPredicate).toArray(BiPredicate[]::new);
            if (isAny)
                return (lt, rt) -> {
                    for (BiPredicate<? super Row, ? super Row> predicate : predicates)
                        if (predicate.test(lt, rt))
                            return true;
                    return false;
                };
            else
                return (lt, rt) -> {
                    for (BiPredicate<? super Row, ? super Row> predicate : predicates)
                        if (!predicate.test(lt, rt))
                            return false;
                    return true;
                };
        }
    }
    
    static class SideMatch extends JoinPred {
        final boolean isLeft;
        final Predicate<? super Row> predicate;
        
        SideMatch(boolean isLeft, Predicate<? super Row> predicate) {
            this.isLeft = isLeft;
            this.predicate = predicate;
        }
    
        void accept(Visitor visitor) {
            visitor.visit(this);
        }
    
        @Override
        BiPredicate<? super Row, ? super Row> toPredicate() {
            return isLeft ? (lt, rt) -> predicate.test(lt)
                : (lt, rt) -> predicate.test(rt);
        }
    }
    
    static class Match extends JoinPred {
        final BiPredicate<? super Row, ? super Row> predicate;
        
        Match(BiPredicate<? super Row, ? super Row> predicate) {
            this.predicate = predicate;
        }
    
        void accept(Visitor visitor) {
            visitor.visit(this);
        }
    
        @Override
        BiPredicate<? super Row, ? super Row> toPredicate() {
            return predicate;
        }
    }
    
    @SuppressWarnings("unchecked")
    private static <T> T cast(Object o) {
        return (T) o;
    }
    
    interface Visitor {
        void visit(Not pred);
        void visit(Binary pred);
        void visit(AnyAll pred);
        void visit(SideMatch pred);
        void visit(Match pred);
    }
    
    static class Simplifier implements Visitor {
        JoinPred output;
        boolean isNotted = false;
        
        @Override
        public void visit(Not pred) {
            isNotted = !isNotted;
            pred.pred.accept(this);
            isNotted = !isNotted;
        }
        
        @Override
        public void visit(Binary pred) {
            if (isNotted) {
                switch (pred.op) {
                    case EQ:  output = new Binary(Binary.Op.NEQ, pred.left, pred.right); break;
                    case NEQ: output = new Binary(Binary.Op.EQ,  pred.left, pred.right); break;
                    case GT:  output = new Binary(Binary.Op.LTE, pred.left, pred.right); break;
                    case GTE: output = new Binary(Binary.Op.LT,  pred.left, pred.right); break;
                    case LT:  output = new Binary(Binary.Op.GTE, pred.left, pred.right); break;
                    case LTE: output = new Binary(Binary.Op.GT,  pred.left, pred.right); break;
                    default: throw new AssertionError();
                }
            } else
                output = pred;
        }
    
        @Override
        public void visit(AnyAll pred) {
            List<JoinPred> children = new ArrayList<>(pred.preds.size());
            visit(pred, children);
            output = new AnyAll(pred.isAny ^ isNotted, children); // Flip isAny if notted
        }
        
        private void visit(AnyAll pred, List<JoinPred> preds) {
            for (JoinPred child : pred.preds) {
                if (child instanceof AnyAll && ((AnyAll) child).isAny == pred.isAny) {
                    visit((AnyAll) child, preds); // Flatten children into same list
                } else {
                    child.accept(this);
                    preds.add(output);
                }
            }
        }
        
        @Override
        public void visit(SideMatch pred) {
            if (isNotted)
                output = new SideMatch(pred.isLeft, pred.predicate.negate());
            else
                output = pred;
        }
        
        @Override
        public void visit(Match pred) {
            if (isNotted)
                output = new Match(pred.predicate.negate());
            else
                output = pred;
        }
    }
    
    static class Compiler implements Visitor {
    
        @Override
        public void visit(Not pred) {
            throw new UnsupportedOperationException();
        }
    
        @Override
        public void visit(Binary pred) {
        
        }
    
        @Override
        public void visit(AnyAll pred) {
        
        }
    
        @Override
        public void visit(SideMatch pred) {
        
        }
    
        @Override
        public void visit(Match pred) {
        
        }
    }
}
