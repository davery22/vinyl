package da.tasets;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static da.tasets.UnsafeUtils.DEFAULT_COMPARATOR;
import static da.tasets.UnsafeUtils.cast;

public class JoinPred {
    JoinPred() {} // Prevent default public constructor
    
    // TODO: Temporary kludge used by AnyAll
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
        enum Op {
            EQ {
                @Override
                boolean test(Object a, Object b) {
                    return Objects.equals(a, b);
                }
            },
            NEQ {
                @Override
                boolean test(Object a, Object b) {
                    return !Objects.equals(a, b);
                }
            },
            GT {
                @Override
                boolean test(Object a, Object b) {
                    return DEFAULT_COMPARATOR.compare(a, b) > 0;
                }
            },
            GTE {
                @Override
                boolean test(Object a, Object b) {
                    return DEFAULT_COMPARATOR.compare(a, b) >= 0;
                }
            },
            LT {
                @Override
                boolean test(Object a, Object b) {
                    return DEFAULT_COMPARATOR.compare(a, b) < 0;
                }
            },
            LTE {
                @Override
                boolean test(Object a, Object b) {
                    return DEFAULT_COMPARATOR.compare(a, b) <= 0;
                }
            },
            ;
            
            Op negate() {
                switch (this) {
                    case EQ: return NEQ;
                    case NEQ: return EQ;
                    case GT: return LTE;
                    case GTE: return LT;
                    case LT: return GTE;
                    case LTE: return GT;
                    default: throw new AssertionError(); // unreachable
                }
            }
            
            abstract boolean test(Object a, Object b);
        }
    
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
            if (side() == JoinAPI.NONE) {
                Object leftVal = ((JoinExpr.Expr<?>) left).supplier.get();
                Object rightVal = ((JoinExpr.Expr<?>) right).supplier.get();
                return op.test(leftVal, rightVal)
                    ? (lt, rt) -> true
                    : (lt, rt) -> false;
            }
            else if (left.side == JoinAPI.NONE) {
                Object val = ((JoinExpr.Expr<?>) left).supplier.get();
                Function<? super Row, ?> mapper = ((JoinExpr.RowExpr<?>) right).mapper;
                return right.side == JoinAPI.LEFT
                    ? (lt, rt) -> op.test(val, mapper.apply(lt))
                    : (lt, rt) -> op.test(val, mapper.apply(rt));
            }
            else if (right.side == JoinAPI.NONE) {
                Function<? super Row, ?> mapper = ((JoinExpr.RowExpr<?>) left).mapper;
                Object val = ((JoinExpr.Expr<?>) right).supplier.get();
                return left.side == JoinAPI.LEFT
                    ? (lt, rt) -> op.test(mapper.apply(lt), val)
                    : (lt, rt) -> op.test(mapper.apply(rt), val);
            }
            else {
                Function<? super Row, ?> leftMapper = ((JoinExpr.RowExpr<?>) left).mapper;
                Function<? super Row, ?> rightMapper = ((JoinExpr.RowExpr<?>) right).mapper;
                return left.side == JoinAPI.LEFT
                    ? right.side == JoinAPI.LEFT
                        ? (lt, rt) -> op.test(leftMapper.apply(lt), rightMapper.apply(lt))
                        : (lt, rt) -> op.test(leftMapper.apply(lt), rightMapper.apply(rt))
                    : right.side == JoinAPI.LEFT
                        ? (lt, rt) -> op.test(leftMapper.apply(rt), rightMapper.apply(lt))
                        : (lt, rt) -> op.test(leftMapper.apply(rt), rightMapper.apply(rt));
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
            return isLeft ? (lt, rt) -> predicate.test(lt) : (lt, rt) -> predicate.test(rt);
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
    
    interface Visitor {
        void visit(Not pred);
        void visit(Binary pred);
        void visit(AnyAll pred);
        void visit(SideMatch pred);
        void visit(Match pred);
    }
    
    /**
     * Removes `Not`s by pushing them down, and flattens nested `AnyAll`s of the same kind.
     */
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
            output = isNotted ? new Binary(pred.op.negate(), pred.left, pred.right) : pred;
        }
    
        @Override
        public void visit(AnyAll pred) {
            if (pred.preds.size() == 1) { // Remove redundant wrapper, eg: any(eq(a,b)) -> eq(a,b)
                pred.preds.get(0).accept(this);
                return;
            }
            boolean isAny = pred.isAny ^ isNotted; // Flip isAny if notted
            List<JoinPred> children = new ArrayList<>(pred.preds.size());
            for (JoinPred child : pred.preds) {
                child.accept(this);
                if (output instanceof AnyAll) {
                    AnyAll ch = (AnyAll) output;
                    if (ch.isAny == isAny) { // Flatten same kind, eg: any(a,any(b,c),d) -> any(a,b,c,d)
                        children.addAll(ch.preds);
                        continue;
                    }
                    else if (ch.preds.isEmpty()) // Always pass/fail, eg: any(a,all(),b) -> all();  all(a,any(),b) -> any()
                        return;
                    // Fall-through to simple case
                }
                children.add(output);
            }
            output = new AnyAll(isAny, children);
        }
        
        @Override
        public void visit(SideMatch pred) {
            output = isNotted ? new SideMatch(pred.isLeft, pred.predicate.negate()) : pred;
        }
        
        @Override
        public void visit(Match pred) {
            output = isNotted ? new Match(pred.predicate.negate()) : pred;
        }
    }
    
    static class IndexCreator implements Visitor {
        final DatasetStream rStream;
        final boolean retainUnmatchedRight;
        JoinAPI.Index output;
        
        IndexCreator(DatasetStream rStream, boolean retainUnmatchedRight) {
            this.rStream = rStream;
            this.retainUnmatchedRight = retainUnmatchedRight;
        }
    
        @Override
        public void visit(Not pred) {
            throw new AssertionError("`Not`s should be removed before this point");
        }
    
        @Override
        public void visit(Binary pred) {
            switch (pred.side()) {
                case 0: /* NONE */ {
                    Object leftVal = ((JoinExpr.Expr<?>) pred.left).supplier.get();
                    Object rightVal = ((JoinExpr.Expr<?>) pred.right).supplier.get();
                    if (pred.op.test(leftVal, rightVal)) {
                        // Entire right side is matched.
                        Row[] rightSide;
                        try (DatasetStream ds = rStream) {
                            // TODO: Only converting because JoinAPI expects FlaggedRows during right/full joins.
                            Stream<Row> s = retainUnmatchedRight ? ds.stream.map(JoinAPI.FlaggedRow::new) : ds.stream;
                            rightSide = s.toArray(Row[]::new);
                        }
                        output = new JoinAPI.Index() {
                            @Override
                            public Stream<Row> unmatchedRight() {
                                return Stream.empty();
                            }
                            
                            @Override
                            public void search(Row left, Consumer<Row> rx) {
                                for (Row right : rightSide)
                                    rx.accept(right);
                            }
                        };
                    }
                    else {
                        // Entire right side is unmatched.
                        if (!retainUnmatchedRight)
                            rStream.close();
                        output = new JoinAPI.Index() {
                            @Override
                            public Stream<Row> unmatchedRight() {
                                return rStream;
                            }
                            
                            @Override
                            public void search(Row left, Consumer<Row> rx) {
                                // Nothing matches
                            }
                        };
                    }
                    break;
                }
                case 1: /* RIGHT */ {
                    doRightSideMatch(singleSidePredicate(pred));
                    break;
                }
                case 2: /* LEFT */ {
                    doLeftSideMatch(singleSidePredicate(pred));
                    break;
                }
                case 3: /* BOTH */ {
                    Function<? super Row, ?> leftMapper;
                    Function<? super Row, ?> rightMapper;
                    if (pred.left.side == JoinAPI.LEFT) {
                        leftMapper = ((JoinExpr.RowExpr<?>) pred.left).mapper;
                        rightMapper = ((JoinExpr.RowExpr<?>) pred.right).mapper;
                    }
                    else {
                        leftMapper = ((JoinExpr.RowExpr<?>) pred.right).mapper;
                        rightMapper = ((JoinExpr.RowExpr<?>) pred.left).mapper;
                    }
                    if (pred.op == Binary.Op.EQ || pred.op == Binary.Op.NEQ) {
                        Map<Object, List<Row>> indexedRight;
                        try (DatasetStream ds = rStream) {
                            Stream<Row> s = retainUnmatchedRight ? ds.stream.map(JoinAPI.FlaggedRow::new) : ds.stream;
                            indexedRight = s.collect(Collectors.groupingBy(rightMapper));
                        }
                        if (pred.op == Binary.Op.EQ) {
                            output = new JoinAPI.Index() {
                                @Override
                                public Stream<Row> unmatchedRight() {
                                    return unjoinedRows(indexedRight.values().stream().flatMap(Collection::stream));
                                }
                                
                                @Override
                                public void search(Row left, Consumer<Row> rx) {
                                    Object leftVal = leftMapper.apply(left);
                                    List<Row> rows = indexedRight.get(leftVal);
                                    if (rows != null)
                                        for (Row row : rows)
                                            rx.accept(row);
                                }
                            };
                        }
                        else { // NEQ
                            output = new JoinAPI.Index() {
                                @Override
                                public Stream<Row> unmatchedRight() {
                                    return unjoinedRows(indexedRight.values().stream().flatMap(Collection::stream));
                                }
                                
                                @Override
                                public void search(Row left, Consumer<Row> rx) {
                                    Object leftVal = leftMapper.apply(left);
                                    indexedRight.forEach((rightVal, rows) -> {
                                        if (!Objects.equals(leftVal, rightVal))
                                            for (Row row : rows)
                                                rx.accept(row);
                                    });
                                }
                            };
                        }
                    }
                    else {
                        // We interpret the operator as <left> <op> <right>, so if the actual
                        // sides are reversed, we negate the operator to handle correctly.
                        Binary.Op op = pred.left.side == JoinAPI.LEFT ? pred.op : pred.op.negate();
                        boolean inclusive = op == Binary.Op.GTE || pred.op == Binary.Op.LTE;
                        TreeMap<Object, List<Row>> indexedRight;
                        try (DatasetStream ds = rStream) {
                            Stream<Row> s = retainUnmatchedRight ? ds.stream.map(JoinAPI.FlaggedRow::new) : ds.stream;
                            indexedRight = s.collect(Collectors.groupingBy(rightMapper, () -> new TreeMap<>(DEFAULT_COMPARATOR), Collectors.toList()));
                        }
                        abstract class TreeIndex implements JoinAPI.Index {
                            @Override
                            public Stream<Row> unmatchedRight() {
                                return unjoinedRows(indexedRight.values().stream().flatMap(Collection::stream));
                            }
                        }
                        switch (op) {
                            case GT, GTE: {
                                output = new TreeIndex() {
                                    @Override
                                    public void search(Row left, Consumer<Row> rx) {
                                        Object leftVal = leftMapper.apply(left);
                                        indexedRight.headMap(leftVal, inclusive).forEach((v, rows) -> {
                                            for (Row row : rows)
                                                rx.accept(row);
                                        });
                                    }
                                };
                                break;
                            }
                            case LT, LTE: {
                                output = new TreeIndex() {
                                    @Override
                                    public void search(Row left, Consumer<Row> rx) {
                                        Object leftVal = leftMapper.apply(left);
                                        indexedRight.tailMap(leftVal, inclusive).forEach((v, rows) -> {
                                            for (Row row : rows)
                                                rx.accept(row);
                                        });
                                    }
                                };
                                break;
                            }
                            default: {
                                throw new AssertionError(); // unreachable
                            }
                        }
                    }
                    break;
                }
                default: {
                    throw new AssertionError(); // unreachable
                }
            }
        }
    
        @Override
        public void visit(AnyAll pred) {
            // TODO: Optimize. A lot.
            BiPredicate<? super Row, ? super Row> predicate = pred.toPredicate();
            Row[] rightSide;
            try (DatasetStream ds = rStream) {
                Stream<Row> s = retainUnmatchedRight ? ds.stream.map(JoinAPI.FlaggedRow::new) : ds.stream;
                rightSide = s.toArray(Row[]::new);
            }
            output = new JoinAPI.Index() {
                @Override
                public Stream<Row> unmatchedRight() {
                    return unjoinedRows(Arrays.stream(rightSide));
                }
        
                @Override
                public void search(Row left, Consumer<Row> rx) {
                    for (Row right : rightSide)
                        if (predicate.test(left, right))
                            rx.accept(right);
                }
            };
        }
    
        @Override
        public void visit(SideMatch pred) {
            if (pred.isLeft)
                doLeftSideMatch(pred.predicate);
            else // isRight
                doRightSideMatch(pred.predicate);
        }
    
        @Override
        public void visit(Match pred) {
            BiPredicate<? super Row, ? super Row> predicate = pred.predicate;
            Row[] rightSide;
            try (DatasetStream ds = rStream) {
                Stream<Row> s = retainUnmatchedRight ? ds.stream.map(JoinAPI.FlaggedRow::new) : ds.stream;
                rightSide = s.toArray(Row[]::new);
            }
            output = new JoinAPI.Index() {
                @Override
                public Stream<Row> unmatchedRight() {
                    return unjoinedRows(Arrays.stream(rightSide));
                }
    
                @Override
                public void search(Row left, Consumer<Row> rx) {
                    for (Row right : rightSide)
                        if (predicate.test(left, right))
                            rx.accept(right);
                }
            };
        }
        
        private void doLeftSideMatch(Predicate<? super Row> predicate) {
            Row[] rightSide;
            try (DatasetStream ds = rStream) {
                Stream<Row> s = retainUnmatchedRight ? ds.stream.map(JoinAPI.FlaggedRow::new) : ds.stream;
                rightSide = s.toArray(Row[]::new);
            }
            output = new JoinAPI.Index() {
                @Override
                public Stream<Row> unmatchedRight() {
                    return unjoinedRows(Arrays.stream(rightSide));
                }
        
                @Override
                public void search(Row left, Consumer<Row> rx) {
                    if (predicate.test(left))
                        for (Row right : rightSide)
                            rx.accept(right);
                }
            };
        }
        
        private void doRightSideMatch(Predicate<? super Row> predicate) {
            List<Row> rightSide;
            List<Row> retainedRight;
            try (DatasetStream ds = rStream) {
                if (retainUnmatchedRight) {
                    // TODO: Only converting because JoinAPI expects FlaggedRows during right/full joins.
                    Map<Boolean, List<Row>> partition = ds.stream
                        .map(JoinAPI.FlaggedRow::new)
                        .collect(Collectors.partitioningBy(predicate));
                    rightSide = partition.get(true);
                    retainedRight = partition.get(false);
                }
                else {
                    rightSide = ds.stream.filter(predicate).collect(Collectors.toList());
                    retainedRight = Collections.emptyList();
                }
            }
            output = new JoinAPI.Index() {
                @Override
                public Stream<Row> unmatchedRight() {
                    return unjoinedRows(Stream.concat(rightSide.stream(), retainedRight.stream()));
                }
        
                @Override
                public void search(Row left, Consumer<Row> rx) {
                    for (Row right : rightSide)
                        rx.accept(right);
                }
            };
        }
        
        private static Stream<Row> unjoinedRows(Stream<Row> stream) {
            Stream<JoinAPI.FlaggedRow> s = cast(stream);
            return s.filter(row -> !row.isJoined).map(row -> row.row);
        }
        
        private static Predicate<? super Row> singleSidePredicate(Binary pred) {
            assert pred.side() == JoinAPI.RIGHT || pred.side() == JoinAPI.LEFT;
            Binary.Op op = pred.op;
            if (pred.left.side == JoinAPI.NONE) {
                Object val = ((JoinExpr.Expr<?>) pred.left).supplier.get();
                Function<? super Row, ?> mapper = ((JoinExpr.RowExpr<?>) pred.right).mapper;
                return row -> op.test(val, mapper.apply(row));
            }
            else if (pred.right.side == JoinAPI.NONE) {
                Function<? super Row, ?> mapper = ((JoinExpr.RowExpr<?>) pred.left).mapper;
                Object val = ((JoinExpr.Expr<?>) pred.right).supplier.get();
                return row -> op.test(mapper.apply(row), val);
            }
            else {
                Function<? super Row, ?> mapper1 = ((JoinExpr.RowExpr<?>) pred.left).mapper;
                Function<? super Row, ?> mapper2 = ((JoinExpr.RowExpr<?>) pred.right).mapper;
                return row -> op.test(mapper1.apply(row), mapper2.apply(row));
            }
        }
    }
}
