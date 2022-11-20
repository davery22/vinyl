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

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A configurator used to define a conversion from input elements to {@link Record records}.
 *
 * <p>Fields defined on the configurator become fields on a resultant {@link Header header}. The header fields are in
 * order of definition on the configurator. A field may be redefined on the configurator, in which case its definition
 * is replaced, but its original position in the header is retained.
 *
 * @see RecordStream.Aux#mapToRecord
 * @see RecordSet#collector
 * @param <T> the type of input elements
 */
public class IntoAPI<T> {
    private final Map<Field<?>, Integer> indexByField = new HashMap<>();
    private final List<Mapper<T>> definitions = new ArrayList<>();
    
    IntoAPI() {} // Prevent default public constructor
    
    /**
     * Defines (or redefines) the given field as the application of the given function to each input object.
     *
     * @param field the field
     * @param mapper a function to apply to each input object
     * @return this configurator
     * @param <U> the value type of the field
     */
    public <U> IntoAPI<T> field(Field<U> field, Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(mapper);
        int index = indexByField.computeIfAbsent(field, k -> definitions.size());
        Mapper<T> def = new Mapper<>(index, mapper);
        if (index == definitions.size())
            definitions.add(def);
        else
            definitions.set(index, def);
        return this;
    }
    
    RecordStream accept(RecordStream.Aux<T> stream, Consumer<IntoAPI<T>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(indexByField);
        @SuppressWarnings("unchecked")
        Mapper<T>[] finalMappers = definitions.toArray(new Mapper[0]);
        
        // Prep the stream transformation.
        int size = finalMappers.length;
        Header nextHeader = new Header(finalIndexByField);
        Stream<Record> nextStream = stream.stream.map(it -> {
            Object[] arr = new Object[size];
            for (Mapper<T> mapper : finalMappers)
                mapper.accept(it, arr);
            return new Record(nextHeader, arr);
        });
    
        return new RecordStream(nextHeader, nextStream);
    }
    
    Collector<T, ?, RecordSet> collector(Consumer<IntoAPI<T>> config) {
        config.accept(this);
    
        // Avoid picking up side-effects from bad-actor callbacks.
        Map<Field<?>, Integer> finalIndexByField = new HashMap<>(indexByField);
        @SuppressWarnings("unchecked")
        Mapper<T>[] finalMappers = definitions.toArray(new Mapper[0]);
        
        int size = definitions.size();
        return Collector.of(
            () -> new ArrayList<Object[]>(),
            (a, t) -> {
                Object[] arr = new Object[size];
                for (Mapper<T> mapper : finalMappers)
                    mapper.accept(t, arr);
                a.add(arr);
            },
            (a, b) -> {
                a.addAll(b);
                return a;
            },
            a -> new RecordSet(new Header(finalIndexByField), a.toArray(new Object[0][]))
        );
    }
    
    private static class Mapper<T> {
        final int index;
        final Function<? super T, ?> mapper;
        
        Mapper(int index, Function<? super T, ?> mapper) {
            this.index = index;
            this.mapper = mapper;
        }
        
        void accept(T in, Object[] arr) {
            arr[index] = mapper.apply(in);
        }
    }
}
