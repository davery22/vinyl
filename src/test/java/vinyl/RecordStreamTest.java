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

package vinyl;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.summarizingInt;
import static java.util.stream.Collectors.summingInt;

public class RecordStreamTest {
    private static final Field<Integer> FIELD_A = new Field<>("A");
    private static final Field<Integer> FIELD_B = new Field<>("B");
    private static final Field<Integer> FIELD_C = new Field<>("C");
    private static final Field<Integer> FIELD_D = new Field<>("D");
    private static final Field<Integer> FIELD_E = new Field<>("E");
    private static final Field<Long> FIELD_F = new Field<>("F");
    private static final Field<Long> FIELD_G = new Field<>("G");
    private static final Field<Double> FIELD_H = new Field<>("H");
    private static final Field<String> FIELD_S = new Field<>("S");
    
    @Test
    void test() {
        RecordStream stream = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .mapToRecord(select -> select
                .field(FIELD_A, i -> i + 1)
                .field(FIELD_B, i -> i + 2)
                .field(FIELD_D, i -> i + 3)
            )
            .select(select -> select
                .allFieldsExcept(FIELD_D)
                .field(FIELD_C, FIELD_D::get)
            );
        FieldPin<Integer> pinB = stream.header().pin(FIELD_B);
        FieldPin<Integer> pinC = stream.header().pin(FIELD_C);
        RecordSet data = stream
            .aggregate(aggregate -> aggregate
                .keyField(FIELD_A)
                .key(o -> o.get(pinB) + o.get(pinC))
                .aggField(FIELD_C, summingInt(FIELD_A::get))
                .aggs(summarizingInt(FIELD_B::get), aggs -> aggs
                    .aggField(FIELD_D, IntSummaryStatistics::getMax)
                    .aggField(FIELD_E, IntSummaryStatistics::getMin)
                    .aggField(FIELD_F, IntSummaryStatistics::getSum)
                )
            )
            .toRecordSet();
        
        System.out.println(data);
    }
    
    @Test
    void testWindowFunction1() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(FIELD_A, i -> i*2))
            .select(select -> select
                .field(FIELD_A)
                .window(window -> window
                    .field(FIELD_B, Analytics.link(FIELD_A::get, -1, 404))
                    .field(FIELD_F, Comparator.comparingInt(FIELD_A::get).reversed(), Analytics.rowNumber(1))
                    .fields(Comparator.comparingInt(FIELD_A::get), fields -> fields
                        .field(FIELD_G, Analytics.percentileDisc(0.500000000, o -> o.get(FIELD_A).longValue()))
                        .field(FIELD_H, Analytics.percentileCont(0.500000000, FIELD_A::get))
                    )
                    .field(FIELD_C, (o, sink) -> sink.accept(o.stream().mapToInt(FIELD_A::get).sum()))
                )
                .window(window -> window
                    .key(o -> o.get(FIELD_A) / 10)
                    .field(FIELD_D, (o, sink) -> sink.accept(o.stream().mapToInt(FIELD_A::get).max().getAsInt()))
                )
            )
            .parallel()
            .toRecordSet();
        
        System.out.println(data);
    }
    
    @Test
    void testJoin1() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 1000)).boxed()
            .mapToRecord(select -> select.field(FIELD_A, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .select(select -> select
                .field(FIELD_A)
                .field(FIELD_S, o -> "Hello, " + o.get(FIELD_A))
            )
            .filter(o -> o.get(FIELD_A) > 20)
            .fullJoin(data.stream()
                          .select(select -> select
                              .field(FIELD_A)
                              .field(FIELD_S, o -> "Goodbye, " + o.get(FIELD_A))
                          )
                          .filter(o -> o.get(FIELD_A) < 980),
                      on -> on.eq(on.left(FIELD_A), on.right(FIELD_A)),
                      select -> select
                          .field(FIELD_A, (l, r) -> l.isNil() ? r.get(FIELD_A) : l.get(FIELD_A))
                          .field(FIELD_S, (l, r) -> l.get(FIELD_S) + " and " + r.get(FIELD_S))
            )
            .toRecordSet();
        
        System.out.println(joined);
    }
    
    @Test
    void testtest() {
        Record record = record(init -> init
            .field(FIELD_A, i -> 1)
            .field(FIELD_B, i -> 2)
            .field(FIELD_C, i -> 3)
        );
    }
    
    Record record(Consumer<IntoAPI<Void>> config) {
        return RecordStream.aux(Stream.of((Void) null)).mapToRecord(config).findFirst().get();
    }
    
    // test collecting objects into a RecordSet
    // test collecting records into multiple RecordSets
    // test some parallel joins
    // test some parallel selects, with or without windows
    // test some parallel aggregates
    // test inner join
    // test left join
    // test right join
    // test full join
    // **test several variations of join conditions
    // test collecting to a record-set, re-streaming, and collecting again, equals
    // test that creating/using field pins from stream headers works
    // test that record values are correct / in order
    // test an aggregate operation with no keys
    // test an aggregate operation with keys
    // test an aggregate operation with multi-keys
    // test an aggregate operation with multi-fields
    // test a window function with no keys
    // test a window function with keys
    // test a select operation with multi-fields
    // test a window function with multi-keys
    // test a window function with multi-fields
    // test that field order matches definition order, even with redefinitions
    // test that analytic function cannot mutate the list passed to it
}
