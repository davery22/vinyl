/*
 * MIT License
 *
 * Copyright (c) 2022 Daniel Avery
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.avery.vinyl;

import io.avery.vinyl.Record.FlaggedRecord;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An opaque value representing a predicate defined by {@link JoinAPI.On}.
 */
public class JoinPred {
    JoinPred() {} // Prevent default public constructor
    
    // Temporary(?) kludge used by AnyAll
    BiPredicate<? super Record, ? super Record> toPredicate() {
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
            CEQ {
                @Override
                boolean test(Object a, Object b) {
                    return Utils.DEFAULT_COMPARATOR.compare(a, b) == 0;
                }
            },
            CNEQ {
                @Override
                boolean test(Object a, Object b) {
                    return Utils.DEFAULT_COMPARATOR.compare(a, b) != 0;
                }
            },
            GT {
                @Override
                boolean test(Object a, Object b) {
                    return Utils.DEFAULT_COMPARATOR.compare(a, b) > 0;
                }
            },
            GTE {
                @Override
                boolean test(Object a, Object b) {
                    return Utils.DEFAULT_COMPARATOR.compare(a, b) >= 0;
                }
            },
            LT {
                @Override
                boolean test(Object a, Object b) {
                    return Utils.DEFAULT_COMPARATOR.compare(a, b) < 0;
                }
            },
            LTE {
                @Override
                boolean test(Object a, Object b) {
                    return Utils.DEFAULT_COMPARATOR.compare(a, b) <= 0;
                }
            },
            ;
            
            Op negate() {
                switch (this) {
                    case EQ: return NEQ;
                    case NEQ: return EQ;
                    case CEQ: return CNEQ;
                    case CNEQ: return CEQ;
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
            this.left = Objects.requireNonNull(left);
            this.right = Objects.requireNonNull(right);
        }
    
        void accept(Visitor visitor) {
            visitor.visit(this);
        }
    
        int side() {
            return left.side | right.side;
        }
    
        @Override
        BiPredicate<? super Record, ? super Record> toPredicate() {
            if (side() == JoinAPI.NONE) {
                Object leftVal = ((JoinExpr.Expr<?>) left).supplier.get();
                Object rightVal = ((JoinExpr.Expr<?>) right).supplier.get();
                return op.test(leftVal, rightVal)
                    ? (lt, rt) -> true
                    : (lt, rt) -> false;
            }
            else if (left.side == JoinAPI.NONE) {
                Object val = ((JoinExpr.Expr<?>) left).supplier.get();
                Function<? super Record, ?> mapper = ((JoinExpr.RecordExpr<?>) right).mapper;
                return right.side == JoinAPI.LEFT
                    ? (lt, rt) -> op.test(val, mapper.apply(lt))
                    : (lt, rt) -> op.test(val, mapper.apply(rt));
            }
            else if (right.side == JoinAPI.NONE) {
                Function<? super Record, ?> mapper = ((JoinExpr.RecordExpr<?>) left).mapper;
                Object val = ((JoinExpr.Expr<?>) right).supplier.get();
                return left.side == JoinAPI.LEFT
                    ? (lt, rt) -> op.test(mapper.apply(lt), val)
                    : (lt, rt) -> op.test(mapper.apply(rt), val);
            }
            else {
                Function<? super Record, ?> leftMapper = ((JoinExpr.RecordExpr<?>) left).mapper;
                Function<? super Record, ?> rightMapper = ((JoinExpr.RecordExpr<?>) right).mapper;
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
        BiPredicate<? super Record, ? super Record> toPredicate() {
            @SuppressWarnings("unchecked")
            BiPredicate<? super Record, ? super Record>[] predicates = preds.stream().map(JoinPred::toPredicate).toArray(BiPredicate[]::new);
            if (isAny)
                return (lt, rt) -> {
                    for (BiPredicate<? super Record, ? super Record> predicate : predicates)
                        if (predicate.test(lt, rt))
                            return true;
                    return false;
                };
            else
                return (lt, rt) -> {
                    for (BiPredicate<? super Record, ? super Record> predicate : predicates)
                        if (!predicate.test(lt, rt))
                            return false;
                    return true;
                };
        }
    }
    
    static class SideMatch extends JoinPred {
        final boolean isLeft;
        final Predicate<? super Record> predicate;
        
        SideMatch(boolean isLeft, Predicate<? super Record> predicate) {
            this.isLeft = isLeft;
            this.predicate = predicate;
        }
    
        void accept(Visitor visitor) {
            visitor.visit(this);
        }
    
        @Override
        BiPredicate<? super Record, ? super Record> toPredicate() {
            return isLeft ? (lt, rt) -> predicate.test(lt) : (lt, rt) -> predicate.test(rt);
        }
    }
    
    static class Match extends JoinPred {
        final BiPredicate<? super Record, ? super Record> predicate;
        
        Match(BiPredicate<? super Record, ? super Record> predicate) {
            this.predicate = predicate;
        }
    
        void accept(Visitor visitor) {
            visitor.visit(this);
        }
    
        @Override
        BiPredicate<? super Record, ? super Record> toPredicate() {
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
        final RecordStream rStream;
        final boolean retainUnmatchedRight;
        JoinAPI.Index output;
        
        IndexCreator(RecordStream rStream, boolean retainUnmatchedRight) {
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
                        Record[] rightSide;
                        try (RecordStream ds = rStream) {
                            // NOTE: Only converting because JoinAPI expects FlaggedRecords during right/full joins.
                            Stream<Record> s = retainUnmatchedRight ? ds.stream.map(FlaggedRecord::new) : ds.stream;
                            rightSide = s.toArray(Record[]::new);
                        }
                        output = new JoinAPI.Index() {
                            @Override
                            public Stream<Record> unmatchedRight() {
                                return Stream.empty();
                            }
                            
                            @Override
                            public void search(Record left, Consumer<Record> sink) {
                                for (Record right : rightSide)
                                    sink.accept(right);
                            }
                        };
                    }
                    else {
                        // Entire right side is unmatched.
                        if (!retainUnmatchedRight)
                            rStream.close();
                        output = new JoinAPI.Index() {
                            @Override
                            public Stream<Record> unmatchedRight() {
                                return rStream;
                            }
                            
                            @Override
                            public void search(Record left, Consumer<Record> sink) {
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
                    abstract class MapIndex<M extends Map<Object, List<Record>>> implements JoinAPI.Index {
                        final M indexedRight;
                        
                        MapIndex(M indexedRight) {
                            this.indexedRight = indexedRight;
                        }
                        
                        @Override
                        public Stream<Record> unmatchedRight() {
                            return unmatchedRecords(indexedRight.values().stream().flatMap(Collection::stream));
                        }
                    }
                    Function<? super Record, ?> leftMapper;
                    Function<? super Record, ?> rightMapper;
                    if (pred.left.side == JoinAPI.LEFT) {
                        leftMapper = ((JoinExpr.RecordExpr<?>) pred.left).mapper;
                        rightMapper = ((JoinExpr.RecordExpr<?>) pred.right).mapper;
                    }
                    else {
                        leftMapper = ((JoinExpr.RecordExpr<?>) pred.right).mapper;
                        rightMapper = ((JoinExpr.RecordExpr<?>) pred.left).mapper;
                    }
                    switch (pred.op) {
                        case EQ: case NEQ: case CEQ: case CNEQ: {
                            Supplier<Map<Object, List<Record>>> mapFactory =
                                pred.op == Binary.Op.CEQ || pred.op == Binary.Op.CNEQ
                                    ? () -> new TreeMap<>(Utils.DEFAULT_COMPARATOR)
                                    : HashMap::new;
                            Map<Object, List<Record>> indexedRight;
                            try (RecordStream ds = rStream) {
                                Stream<Record> s = retainUnmatchedRight ? ds.stream.map(FlaggedRecord::new) : ds.stream;
                                indexedRight = s.collect(Collectors.groupingBy(rightMapper, mapFactory, Collectors.toList()));
                            }
                            if (pred.op == Binary.Op.EQ || pred.op == Binary.Op.CEQ) {
                                output = new MapIndex<>(indexedRight) {
                                    @Override
                                    public void search(Record left, Consumer<Record> sink) {
                                        Object leftVal = leftMapper.apply(left);
                                        List<Record> records = indexedRight.get(leftVal);
                                        if (records != null)
                                            for (Record record : records)
                                                sink.accept(record);
                                    }
                                };
                            }
                            else { // NEQ, CNEQ
                                Binary.Op op = pred.op;
                                output = new MapIndex<>(indexedRight) {
                                    @Override
                                    public void search(Record left, Consumer<Record> sink) {
                                        Object leftVal = leftMapper.apply(left);
                                        indexedRight.forEach((rightVal, records) -> {
                                            if (op.test(leftVal, rightVal))
                                                for (Record record : records)
                                                    sink.accept(record);
                                        });
                                    }
                                };
                            }
                            break;
                        }
                        default: {
                            // We interpret the operator as <left> <op> <right>, so if the actual
                            // sides are reversed, we negate the operator to handle correctly.
                            Binary.Op op = (pred.left.side == JoinAPI.LEFT) ? pred.op : pred.op.negate();
                            boolean inclusive = op == Binary.Op.GTE || op == Binary.Op.LTE;
                            TreeMap<Object, List<Record>> indexedRight;
                            try (RecordStream ds = rStream) {
                                Stream<Record> s = retainUnmatchedRight ? ds.stream.map(FlaggedRecord::new) : ds.stream;
                                indexedRight = s.collect(Collectors.groupingBy(rightMapper, () -> new TreeMap<>(Utils.DEFAULT_COMPARATOR), Collectors.toList()));
                            }
                            switch (op) {
                                case GT: case GTE: {
                                    output = new MapIndex<>(indexedRight) {
                                        @Override
                                        public void search(Record left, Consumer<Record> sink) {
                                            Object leftVal = leftMapper.apply(left);
                                            indexedRight.headMap(leftVal, inclusive).forEach((v, records) -> {
                                                for (Record record : records)
                                                    sink.accept(record);
                                            });
                                        }
                                    };
                                    break;
                                }
                                case LT: case LTE: {
                                    output = new MapIndex<>(indexedRight) {
                                        @Override
                                        public void search(Record left, Consumer<Record> sink) {
                                            Object leftVal = leftMapper.apply(left);
                                            indexedRight.tailMap(leftVal, inclusive).forEach((v, records) -> {
                                                for (Record record : records)
                                                    sink.accept(record);
                                            });
                                        }
                                    };
                                }
                            } // end inner op switch
                        }
                    } // end outer op switch
                }
            } // end side switch
        }
    
        @Override
        public void visit(AnyAll pred) {
            // TODO: Optimize. A lot.
            BiPredicate<? super Record, ? super Record> predicate = pred.toPredicate();
            Record[] rightSide;
            try (RecordStream ds = rStream) {
                Stream<Record> s = retainUnmatchedRight ? ds.stream.map(FlaggedRecord::new) : ds.stream;
                rightSide = s.toArray(Record[]::new);
            }
            output = new JoinAPI.Index() {
                @Override
                public Stream<Record> unmatchedRight() {
                    return unmatchedRecords(Arrays.stream(rightSide));
                }
        
                @Override
                public void search(Record left, Consumer<Record> sink) {
                    for (Record right : rightSide)
                        if (predicate.test(left, right))
                            sink.accept(right);
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
            BiPredicate<? super Record, ? super Record> predicate = pred.predicate;
            Record[] rightSide;
            try (RecordStream ds = rStream) {
                Stream<Record> s = retainUnmatchedRight ? ds.stream.map(FlaggedRecord::new) : ds.stream;
                rightSide = s.toArray(Record[]::new);
            }
            output = new JoinAPI.Index() {
                @Override
                public Stream<Record> unmatchedRight() {
                    return unmatchedRecords(Arrays.stream(rightSide));
                }
    
                @Override
                public void search(Record left, Consumer<Record> sink) {
                    for (Record right : rightSide)
                        if (predicate.test(left, right))
                            sink.accept(right);
                }
            };
        }
        
        private void doLeftSideMatch(Predicate<? super Record> predicate) {
            Record[] rightSide;
            try (RecordStream ds = rStream) {
                Stream<Record> s = retainUnmatchedRight ? ds.stream.map(FlaggedRecord::new) : ds.stream;
                rightSide = s.toArray(Record[]::new);
            }
            output = new JoinAPI.Index() {
                @Override
                public Stream<Record> unmatchedRight() {
                    return unmatchedRecords(Arrays.stream(rightSide));
                }
        
                @Override
                public void search(Record left, Consumer<Record> sink) {
                    if (predicate.test(left))
                        for (Record right : rightSide)
                            sink.accept(right);
                }
            };
        }
        
        private void doRightSideMatch(Predicate<? super Record> predicate) {
            List<Record> rightSide;
            List<Record> retainedRight;
            try (RecordStream ds = rStream) {
                if (retainUnmatchedRight) {
                    // NOTE: Only converting because JoinAPI expects FlaggedRecords during right/full joins.
                    Map<Boolean, List<Record>> partition = ds.stream
                        .map(FlaggedRecord::new)
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
                public Stream<Record> unmatchedRight() {
                    return unmatchedRecords(Stream.concat(rightSide.stream(), retainedRight.stream()));
                }
        
                @Override
                public void search(Record left, Consumer<Record> sink) {
                    for (Record right : rightSide)
                        sink.accept(right);
                }
            };
        }
        
        private static Stream<Record> unmatchedRecords(Stream<Record> stream) {
            Stream<FlaggedRecord> s = Utils.cast(stream);
            return s.filter(record -> !record.isMatched).map(record -> record.record);
        }
        
        private static Predicate<? super Record> singleSidePredicate(Binary pred) {
            assert pred.side() == JoinAPI.RIGHT || pred.side() == JoinAPI.LEFT;
            Binary.Op op = pred.op;
            if (pred.left.side == JoinAPI.NONE) {
                Object val = ((JoinExpr.Expr<?>) pred.left).supplier.get();
                Function<? super Record, ?> mapper = ((JoinExpr.RecordExpr<?>) pred.right).mapper;
                return record -> op.test(val, mapper.apply(record));
            }
            else if (pred.right.side == JoinAPI.NONE) {
                Function<? super Record, ?> mapper = ((JoinExpr.RecordExpr<?>) pred.left).mapper;
                Object val = ((JoinExpr.Expr<?>) pred.right).supplier.get();
                return record -> op.test(mapper.apply(record), val);
            }
            else {
                Function<? super Record, ?> mapper1 = ((JoinExpr.RecordExpr<?>) pred.left).mapper;
                Function<? super Record, ?> mapper2 = ((JoinExpr.RecordExpr<?>) pred.right).mapper;
                return record -> op.test(mapper1.apply(record), mapper2.apply(record));
            }
        }
    }
}
