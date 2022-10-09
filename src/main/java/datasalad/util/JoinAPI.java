package datasalad.util;

import java.util.*;
import java.util.function.*;

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
        
        // TODO
        throw new UnsupportedOperationException();
    }
    
    public class On {
        On() {} // Prevent default public constructor
    
        public <T extends Comparable<? super T>> JoinExpr<T> left(Column<T> column) {
            return new JoinExpr.LCol<>(left.header.locator(column));
        }
        
        public <T extends Comparable<? super T>> JoinExpr<T> right(Column<T> column) {
            return new JoinExpr.RCol<>(right.header.locator(column));
        }
        
        public <T extends Comparable<? super T>> JoinExpr<T> left(Function<? super Row, T> mapper) {
            return new JoinExpr.LExpr<>(mapper);
        }
        
        public <T extends Comparable<? super T>> JoinExpr<T> right(Function<? super Row, T> mapper) {
            return new JoinExpr.RExpr<>(mapper);
        }
        
        public <T extends Comparable<? super T>> JoinExpr<T> eval(Supplier<T> supplier) {
            return new JoinExpr.Expr<>(supplier);
        }
        
        public <T extends Comparable<? super T>> JoinExpr<T> val(T val) {
            return new JoinExpr.Expr<>(() -> val);
        }
        
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
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select lall() {
            Column<?>[] columns = left.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                Locator locator = new Locator(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> lt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rall() {
            Column<?>[] columns = right.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                Locator locator = new Locator(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> rt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select lallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            Column<?>[] columns = left.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                if (excludedSet.contains(column))
                    continue;
                Locator locator = new Locator(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> lt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            Column<?>[] columns = right.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                if (excludedSet.contains(column))
                    continue;
                Locator locator = new Locator(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> rt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select lcol(Column<?> column) {
            Locator locator = new Locator(column, left.header.indexOf(column));
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, (lt, rt) -> lt.get(locator));
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
    
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Select rcol(Column<?> column) {
            Locator locator = new Locator(column, right.header.indexOf(column));
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, (lt, rt) -> rt.get(locator));
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        public <T extends Comparable<? super T>> Select col(Column<T> column, BiFunction<? super Row, ? super Row, ? extends T> mapper) {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, mapper);
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        public <T> Select cols(BiFunction<? super Row, ? super Row, ? extends T> mapper, Consumer<Cols<T>> config) {
            config.accept(new Cols<>(mapper));
            return this;
        }
        
        private abstract static class Mapper {
            abstract void accept(Row left, Row right, Comparable<?>[] arr);
        }
    
        private static class RowMapper extends Mapper {
            final int index;
            final BiFunction<? super Row, ? super Row, ? extends Comparable<?>> mapper;
        
            RowMapper(int index, BiFunction<? super Row, ? super Row, ? extends Comparable<?>> mapper) {
                this.index = index;
                this.mapper = mapper;
            }
        
            @Override
            void accept(Row left, Row right, Comparable<?>[] arr) {
                arr[index] = mapper.apply(left, right);
            }
        }
        
        public class Cols<T> extends Mapper {
            final BiFunction<? super Row, ? super Row, ? extends T> mapper;
            final List<ObjMapper> children = new ArrayList<>();
    
            Cols(BiFunction<? super Row, ? super Row, ? extends T> mapper) {
                this.mapper = mapper;
            }
    
            public <U extends Comparable<? super U>> Cols<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                ObjMapper def = new ObjMapper(index, mapper);
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
                return this;
            }
    
            @Override
            void accept(Row left, Row right, Comparable<?>[] arr) {
                T obj = mapper.apply(left, right);
                children.forEach(child -> child.accept(obj, arr));
            }
    
            private class ObjMapper {
                final int index;
                final Function<? super T, ? extends Comparable<?>> mapper;
        
                ObjMapper(int index, Function<? super T, ? extends Comparable<?>> mapper) {
                    this.index = index;
                    this.mapper = mapper;
                }
        
                void accept(T in, Comparable<?>[] arr) {
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
