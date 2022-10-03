package datasalad.util;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class SelectAPI {
    private final DatasetStream stream;
    private final Map<Column<?>, Integer> indexByColumn = new HashMap<>();
    private final List<Object> definitions = new ArrayList<>();
    
    SelectAPI(DatasetStream stream) {
        this.stream = stream;
    }
    
    public SelectAPI all() {
        stream.header.columns.forEach(column -> {
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, row -> row.get(column));
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
        });
        return this;
    }
    
    public SelectAPI allExcept(Column<?>... columns) {
        Set<Column<?>> excluded = Set.of(columns);
        stream.header.columns.forEach(column -> {
            if (excluded.contains(column))
                return;
            int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
            RowMapper def = new RowMapper(index, row -> row.get(column));
            if (index == definitions.size())
                definitions.add(def);
            else
                definitions.set(index, def);
        });
        return this;
    }
    
    public SelectAPI col(Column<?> column) {
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        RowMapper def = new RowMapper(index, row -> row.get(column));
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    public <T extends Comparable<T>> SelectAPI col(Column<T> column, Function<? super Row, ? extends T> mapper) {
        int index = indexByColumn.computeIfAbsent(column, k -> definitions.size());
        RowMapper def = new RowMapper(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    public <T> SelectAPI cols(Function<? super Row, ? extends T> mapper, Consumer<Cols<T>> config) {
        config.accept(new Cols<>(mapper));
        return this;
    }
    
    DatasetStream accept(Consumer<SelectAPI> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        // Final mappers will combine ObjMapper children into their parent.
        Map<Column<?>, Integer> finalIndexByColumn = Map.copyOf(indexByColumn);
        List<Mapper> finalMappers = new ArrayList<>();
        Set<Cols<?>> seenParents = new HashSet<>();
        for (Object def : definitions) {
            if (def instanceof RowMapper)
                finalMappers.add((RowMapper) def);
            else {
                assert def instanceof Cols.ObjMapper;
                Cols<?>.ObjMapper child = (Cols<?>.ObjMapper) def;
                child.addToParent();
                if (seenParents.add(child.parent()))
                    finalMappers.add(child.parent());
            }
        }
        
        // Prep the row-by-row transformation.
        int size = definitions.size();
        Header nextHeader = new Header(finalIndexByColumn);
        Stream<Row> nextStream = stream.stream.map(it -> {
            Comparable<?>[] arr = new Comparable[size];
            for (Mapper mapper : finalMappers)
                mapper.accept(it, arr);
            return new Row(nextHeader, arr);
        });
    
        return new DatasetStream(nextHeader, nextStream);
    }
    
    private abstract static class Mapper {
        abstract void accept(Row row, Comparable<?>[] arr);
    }
    
    private static class RowMapper extends Mapper {
        final int index;
        final Function<? super Row, ? extends Comparable<?>> mapper;
        
        RowMapper(int index, Function<? super Row, ? extends Comparable<?>> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        @Override
        void accept(Row row, Comparable<?>[] arr) {
            arr[index] = mapper.apply(row);
        }
    }
    
    public class Cols<T> extends Mapper {
        final Function<? super Row, ? extends T> mapper;
        final List<ObjMapper> children = new ArrayList<>();
        
        Cols(Function<? super Row, ? extends T> mapper) {
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
        void accept(Row row, Comparable<?>[] arr) {
            T obj = mapper.apply(row);
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
