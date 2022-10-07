package datasalad.util;

import java.util.*;
import java.util.function.*;

public class JoinAPI {
    private final JoinType type;
    private final DatasetStream left;
    private final DatasetStream right;
    private On on = null;
    private Select select = null;
    
    enum JoinType { INNER, LEFT, RIGHT, FULL }
    
    JoinAPI(JoinType type, DatasetStream left, DatasetStream right) {
        this.type = type;
        this.left = left;
        this.right = right;
    }
    
    public JoinAPI on(Consumer<On> config) {
        config.accept(on = new On());
        return this;
    }
    
    public JoinAPI andSelect(Consumer<Select> config) {
        config.accept(select = new Select());
        return this;
    }
    
    DatasetStream accept(Consumer<JoinAPI> config) {
        config.accept(this);
        
        if (select == null)
            select = new Select().lall().rall();
        
        // TODO
        throw new UnsupportedOperationException();
    }
    
    public class On {
        On() {} // Prevent default public constructor
    
        public <T extends Comparable<T>> JoinExpr<T> left(Column<T> column) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> right(Column<T> column) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> left(Function<? super Row, T> mapper) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> right(Function<? super Row, T> mapper) {
            return null;
        }
        
        public <T extends Comparable<T>> JoinExpr<T> val(Supplier<T> val) {
            return null;
        }
        
        public JoinPred not(JoinPred predicate) {
            return null;
        }
        
        public JoinPred all(JoinPred... predicates) {
            return null;
        }
        
        public JoinPred any(JoinPred... predicates) {
            return null;
        }
        
        public JoinPred lcheck(Predicate<? super Row> predicate) {
            return null;
        }
        
        public JoinPred rcheck(Predicate<? super Row> predicate) {
            return null;
        }
        
        public JoinPred check(BiPredicate<? super Row, ? super Row> predicate) {
            return null;
        }
    }
    
    public class Select {
        private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
        private final List<Object> definitions = new ArrayList<>();
        
        Select() {} // Prevent default public constructor
        
        public Select lall() {
            Column<?>[] columns = left.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                Locator<?> locator = new Locator<>(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> lt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
        
        public Select rall() {
            Column<?>[] columns = right.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                Locator<?> locator = new Locator<>(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> rt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
        
        public Select lallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            Column<?>[] columns = left.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                if (excludedSet.contains(column))
                    continue;
                Locator<?> locator = new Locator<>(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> lt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
        
        public Select rallExcept(Column<?>... excluded) {
            Set<Column<?>> excludedSet = Set.of(excluded);
            Column<?>[] columns = right.header.columns;
            for (int i = 0; i < columns.length; i++) {
                Column<?> column = columns[i];
                if (excludedSet.contains(column))
                    continue;
                Locator<?> locator = new Locator<>(column, i);
                int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
                RowMapper def = new RowMapper(index, (lt, rt) -> rt.get(locator));
                if (index == definitions.size())
                    definitions.add(def);
                else
                    definitions.set(index, def);
            }
            return this;
        }
        
        public Select lcol(Column<?> column) {
            Locator<?> locator = new Locator<>(column, left.header.indexOf(column));
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, (lt, rt) -> lt.get(locator));
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        public Select rcol(Column<?> column) {
            Locator<?> locator = new Locator<>(column, right.header.indexOf(column));
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, (lt, rt) -> rt.get(locator));
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
            return this;
        }
        
        public <T extends Comparable<T>> Select col(Column<T> column, BiFunction<? super Row, ? super Row, ? extends T> mapper) {
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
    
            public <U extends Comparable<U>> Cols<T> col(Column<U> column, Function<? super T, ? extends U> mapper) {
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
