package da.tasets;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinAPI {
    private final JoinType type;
    private final DatasetStream left;
    private final DatasetStream right;
    private JoinPred pred = null;
    private Select select = null;
    
    enum JoinType { INNER, LEFT, RIGHT, FULL }
    
    // 'Side' values, used by JoinExpr and JoinPred.
    static int NONE = 0;
    static int RIGHT = 1;
    static int LEFT = 2;
    static int BOTH = 3; // LEFT | RIGHT
    
    JoinAPI(JoinType type, DatasetStream left, DatasetStream right) {
        this.type = type;
        this.left = left;
        this.right = right;
    }
    
    public JoinAPI on(Function<On, JoinPred> config) {
        pred = config.apply(new On());
        return this;
    }
    
    public JoinAPI andSelect(Consumer<Select> config) {
        config.accept(select = new Select());
        return this;
    }
    
    DatasetStream accept(Consumer<JoinAPI> config) {
        config.accept(this);
        
        if (pred == null)
            pred = new On().all(); // cross-join
        if (select == null)
            select = new Select().lall().rall(); // merge columns
        
        // left/right/const | ineq/eq
        //
        // right-eq-left           --> point-index right
        // right-ineq-left         --> range-index right
        // right-in/eq-const/right --> pre-filter right
        // rmatch                  --> pre-filter right
        // left-in/eq-const/left   --> filter left
        // lmatch                  --> filter left
        // match                   --> filter both
        //
        // all --> nest consecutive indexes & pre-filters, until match() is reached
        // any --> separate consecutive indexes & pre-filters
        //
        // 1. Describe how to build structure on the right-hand side.
        // 2. Describe how to traverse structure and run checks.
        //
        // TODO:
        //  1. Repeated point-indexes: L(B) = R(C) AND ... AND L(C) = R(C)
        //     -->   L(B) = R(C) = L(C)
        //  2. L(B) < R(C) AND ... AND L(C) > R(C)
        //     -->   L(B) < R(C) < L(C)
        
        // TODO
        throw new UnsupportedOperationException();
    }
    
    public class On {
        On() {} // Prevent default public constructor
    
        public <T> JoinExpr<T> left(Column<T> column) {
            return new JoinExpr.LCol<>(left.header.locator(column));
        }
        
        public <T> JoinExpr<T> right(Column<T> column) {
            return new JoinExpr.RCol<>(right.header.locator(column));
        }
        
        public <T> JoinExpr<T> left(Function<? super Row, T> mapper) {
            return new JoinExpr.LExpr<>(mapper);
        }
        
        public <T> JoinExpr<T> right(Function<? super Row, T> mapper) {
            return new JoinExpr.RExpr<>(mapper);
        }
        
        public <T> JoinExpr<T> eval(Supplier<T> supplier) {
            return new JoinExpr.Expr<>(supplier);
        }
        
        public <T> JoinExpr<T> val(T val) {
            return new JoinExpr.Expr<>(() -> val);
        }
    
    
        public JoinPred eq(JoinExpr<?> left, JoinExpr<?> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.EQ, left, right);
        }
    
        public JoinPred neq(JoinExpr<?> left, JoinExpr<?> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.NEQ, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred gt(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.GT, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred gte(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.GTE, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred lt(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.LT, left, right);
        }
    
        public <T extends Comparable<? super T>> JoinPred lte(JoinExpr<T> left, JoinExpr<T> right) {
            return new JoinPred.Binary(JoinPred.Binary.Op.LTE, left, right);
        }
    
        public JoinPred in(JoinExpr<?> left, JoinExpr<?>... exprs) {
            // TODO: Use Set?
            return new JoinPred.AnyAll(true, Stream.of(exprs).map(right -> eq(left, right)).collect(Collectors.toList()));
        }
    
        public <T extends Comparable<? super T>> JoinPred between(JoinExpr<T> left, JoinExpr<T> start, JoinExpr<T> end) {
            return new JoinPred.AnyAll(false, List.of(gte(left, start), lte(left, end)));
        }
    
        // TODO
//        public JoinPred like(JoinExpr<String> left, Supplier<String> pattern) {
//            throw new UnsupportedOperationException();
//        }
        
        public JoinPred not(JoinPred predicate) {
            return new JoinPred.Not(predicate);
        }
    
        public JoinPred any(JoinPred... predicates) {
            return new JoinPred.AnyAll(true, List.of(predicates));
        }
    
        public JoinPred all(JoinPred... predicates) {
            return new JoinPred.AnyAll(false, List.of(predicates));
        }
        
        public JoinPred lmatch(Predicate<? super Row> predicate) {
            return new JoinPred.SideMatch(true, predicate);
        }
        
        public JoinPred rmatch(Predicate<? super Row> predicate) {
            return new JoinPred.SideMatch(false, predicate);
        }
        
        public JoinPred match(BiPredicate<? super Row, ? super Row> predicate) {
            return new JoinPred.Match(predicate);
        }
    }
    
    public class Select {
        private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
        private final List<Object> definitions = new ArrayList<>();
        
        Select() {} // Prevent default public constructor
        
        public Select lall() {
            for (Column<?> column : left.header.columns)
                lcol(column);
            return this;
        }
    
        public Select rall() {
            for (Column<?> column : right.header.columns)
                rcol(column);
            return this;
        }
    
        public Select lallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            for (Column<?> column : left.header.columns)
                if (!excludedSet.contains(column))
                    lcol(column);
            return this;
        }
    
        public Select rallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            for (Column<?> column : right.header.columns)
                if (!excludedSet.contains(column))
                    rcol(column);
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select lcol(Column<?> column) {
            Locator locator = new Locator(column, left.header.indexOf(column));
            return col((Column) column, (lt, rt) -> lt.get(locator));
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rcol(Column<?> column) {
            Locator locator = new Locator(column, right.header.indexOf(column));
            return col((Column) column, (lt, rt) -> rt.get(locator));
        }
        
        public <T> Select col(Column<T> column, BiFunction<? super Row, ? super Row, ? extends T> mapper) {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        public Select lcols(Column<?>... columns) {
            for (Column<?> column : columns)
                lcol(column);
            return this;
        }
        
        public Select rcols(Column<?>... columns) {
            for (Column<?> column : columns)
                rcol(column);
            return this;
        }
        
        public <T> Select cols(BiFunction<? super Row, ? super Row, ? extends T> mapper, Consumer<Cols<T>> config) {
            config.accept(new Cols<>(mapper));
            return this;
        }
        
        private abstract static class Mapper {
            abstract void accept(Row left, Row right, Object[] arr);
        }
    
        private static class RowMapper extends Mapper {
            final int index;
            final BiFunction<? super Row, ? super Row, ?> mapper;
        
            RowMapper(int index, BiFunction<? super Row, ? super Row, ?> mapper) {
                this.index = index;
                this.mapper = mapper;
            }
        
            @Override
            void accept(Row left, Row right, Object[] arr) {
                arr[index] = mapper.apply(left, right);
            }
        }
        
        public class Cols<T> extends Mapper {
            final BiFunction<? super Row, ? super Row, ? extends T> mapper;
            final List<ObjMapper> children = new ArrayList<>();
    
            Cols(BiFunction<? super Row, ? super Row, ? extends T> mapper) {
                this.mapper = mapper;
            }
    
            public <U> Cols<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                ObjMapper def = new ObjMapper(index, mapper);
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
                return this;
            }
    
            @Override
            void accept(Row left, Row right, Object[] arr) {
                T obj = mapper.apply(left, right);
                children.forEach(child -> child.accept(obj, arr));
            }
    
            private class ObjMapper {
                final int index;
                final Function<? super T, ?> mapper;
        
                ObjMapper(int index, Function<? super T, ?> mapper) {
                    this.index = index;
                    this.mapper = mapper;
                }
        
                void accept(T in, Object[] arr) {
                    arr[index] = mapper.apply(in);
                }
        
                void addToParent() {
                    Cols.this.children.add(this);
                }
        
                Cols<T> parent() {
                    return Cols.this;
                }
            }
        }
    }
}
