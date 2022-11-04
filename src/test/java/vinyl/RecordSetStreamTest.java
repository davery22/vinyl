package vinyl;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.summarizingInt;
import static java.util.stream.Collectors.summingInt;

public class RecordSetStreamTest {
    private static final Field<Integer> FIELD_A = new Field<>("A");
    private static final Field<Integer> FIELD_B = new Field<>("B");
    private static final Field<Integer> FIELD_C = new Field<>("C");
    private static final Field<Integer> FIELD_D = new Field<>("D");
    private static final Field<Integer> FIELD_E = new Field<>("E");
    private static final Field<Long> FIELD_F = new Field<>("F");
    private static final Field<String> FIELD_S = new Field<>("S");
    
    @Test
    void test() {
        RecordStream data$ = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .mapToRecord($->$
                .field(FIELD_A, i -> i + 1)
                .field(FIELD_B, i -> i + 2)
                .field(FIELD_D, i -> i + 3)
            )
            .select($->$
                .allExcept(FIELD_D)
                .field(FIELD_C, FIELD_D::get)
            );
        FieldPin<Integer> pinB = data$.header().pin(FIELD_B);
        FieldPin<Integer> pinC = data$.header().pin(FIELD_C);
        RecordSet data = data$
            .aggregate($->$
                .keyField(FIELD_A)
                .key(o -> o.get(pinB) + o.get(pinC))
                .aggField(FIELD_C, summingInt(FIELD_A::get))
                .aggs(summarizingInt(FIELD_B::get), $$->$$
                    .aggField(FIELD_D, IntSummaryStatistics::getMax)
                    .aggField(FIELD_E, IntSummaryStatistics::getMin)
                    .aggField(FIELD_F, IntSummaryStatistics::getSum)
                )
            )
            .toRecordSet();
        
        System.out.println(data);
        
//        Dataset selfJoined = data.stream()
//            .join(data.stream(), $$->$$
//                .on($-> $.eq($.left(FIELD_A), $.right(FIELD_B)))
//                .on($->
//                    $.not(
//                        $.any(
//                            $.eq($.val(3), $.val("")),
//                            $.gte($.left(FIELD_A), $.right(FIELD_D))
//                        )
//                    )
//                )
//                .andSelect($->$
//                    .lField(FIELD_A)
//                    .rallExcept(FIELD_A)
//                )
//            )
//            .toDataset();
    }
    
    @Test
    void testWindowFunction1() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .mapToRecord($ -> $.field(FIELD_A, i -> i*2))
            .select($$ -> $$
                .field(FIELD_A)
                .window($->$
                    .field(FIELD_B, Comparator.comparingInt(FIELD_A::get).reversed(), (os, rx) -> IntStream.range(0, os.size()).forEach(rx::accept))
                    .field(FIELD_C, (os, rx) -> rx.accept(os.stream().mapToInt(FIELD_A::get).sum()))
                )
                .window($->$
                    .key(o -> o.get(FIELD_A) / 10)
                    .field(FIELD_D, (os, rx) -> rx.accept(os.stream().mapToInt(FIELD_A::get).max().orElseThrow()))
                )
            )
            .parallel()
            .toRecordSet();
        
        System.out.println(data);
    }
    
    @Test
    void testJoin1() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 1000)).boxed()
            .mapToRecord($ -> $.field(FIELD_A, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .select($->$
                .field(FIELD_A)
                .field(FIELD_S, o -> "Hello, " + o.get(FIELD_A))
            )
            .filter(o -> o.get(FIELD_A) > 20)
            .fullJoin(data.stream()
                          .select($->$
                              .field(FIELD_A)
                              .field(FIELD_S, o -> "Goodbye, " + o.get(FIELD_A))
                          )
                          .filter(o -> o.get(FIELD_A) < 980),
                      join -> join
                          .on(on -> on.eq(on.left(FIELD_A), on.right(FIELD_A)))
//                          .on($->$.match((l, r) -> Objects.equals(l.get(FIELD_A), r.get(FIELD_A))))
                          .andSelect($->$
                              .field(FIELD_A, (l, r) -> l.get(FIELD_A) != null ? l.get(FIELD_A) : r.get(FIELD_A))
                              .field(FIELD_S, (l, r) -> l.get(FIELD_S) + " and " + r.get(FIELD_S))
                          )
            )
            .toRecordSet();
        
        System.out.println(joined);
//        if (joined.stream().count() == 0)
//            throw new AssertionError();
    }
}
