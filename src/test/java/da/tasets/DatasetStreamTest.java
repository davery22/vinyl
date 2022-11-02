package da.tasets;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.summarizingInt;
import static java.util.stream.Collectors.summingInt;

public class DatasetStreamTest {
    private static final Column<Integer> COL_A = new Column<>("A");
    private static final Column<Integer> COL_B = new Column<>("B");
    private static final Column<Integer> COL_C = new Column<>("C");
    private static final Column<Integer> COL_D = new Column<>("D");
    private static final Column<Integer> COL_E = new Column<>("E");
    private static final Column<Long> COL_F = new Column<>("F");
    private static final Column<String> COL_S = new Column<>("S");
    
    @Test
    void test() {
        DatasetStream data$ = DatasetStream.aux(IntStream.range(0, 100)).boxed()
            .mapToDataset($->$
                .col(COL_A, i -> i + 1)
                .col(COL_B, i -> i + 2)
                .col(COL_D, i -> i + 3)
            )
            .select($->$
                .allExcept(COL_D)
                .col(COL_C, COL_D::get)
            );
        Locator<Integer> colB = data$.header().locator(COL_B);
        Locator<Integer> colC = data$.header().locator(COL_C);
        Dataset data = data$
            .aggregate($->$
                .keyCol(COL_A)
                .key(row -> row.get(colB) + row.get(colC))
                .aggCol(COL_C, summingInt(COL_A::get))
                .aggs(summarizingInt(COL_B::get), $$->$$
                    .aggCol(COL_D, IntSummaryStatistics::getMax)
                    .aggCol(COL_E, IntSummaryStatistics::getMin)
                    .aggCol(COL_F, IntSummaryStatistics::getSum)
                )
            )
            .toDataset();
        
        System.out.println(data);
        
//        Dataset selfJoined = data.stream()
//            .join(data.stream(), $$->$$
//                .on($-> $.eq($.left(COL_A), $.right(COL_B)))
//                .on($->
//                    $.not(
//                        $.any(
//                            $.eq($.val(3), $.val("")),
//                            $.gte($.left(COL_A), $.right(COL_D))
//                        )
//                    )
//                )
//                .andSelect($->$
//                    .lcol(COL_A)
//                    .rallExcept(COL_A)
//                )
//            )
//            .toDataset();
    }
    
    @Test
    void testWindowFunction1() {
        Dataset data = DatasetStream.aux(IntStream.range(0, 100)).boxed()
            .mapToDataset($ -> $.col(COL_A, i -> i*2))
            .select($$ -> $$
                .col(COL_A)
                .window($->$
                    .col(COL_B, Comparator.comparingInt(COL_A::get).reversed(), (rows, rx) -> IntStream.range(0, rows.size()).forEach(rx::accept))
                    .col(COL_C, (rows, rx) -> rx.accept(rows.stream().mapToInt(COL_A::get).sum()))
                )
                .window($->$
                    .key(row -> row.get(COL_A) / 10)
                    .col(COL_D, (rows, rx) -> rx.accept(rows.stream().mapToInt(COL_A::get).max().orElseThrow()))
                )
            )
            .parallel()
            .toDataset();
        
        System.out.println(data);
    }
    
    @Test
    void testJoin1() {
        Dataset data = DatasetStream.aux(IntStream.range(0, 100)).boxed()
            .mapToDataset($ -> $.col(COL_A, i -> i))
            .toDataset();
        
        Dataset joined = data.stream()
            .select($->$.col(COL_A).col(COL_S, row -> "Hello, " + row.get(COL_A)))
            .join(data.stream()
                      .select($->$.col(COL_A).col(COL_S, row -> "Goodbye, " + row.get(COL_A))),
                  $$->$$
                      .on($->$.lt($.left(COL_A), $.right(COL_A)))
                      .andSelect($->$.lcol(COL_A).col(COL_S, (lt, rt) -> lt.get(COL_S) + " and " + rt.get(COL_S)))
            )
            .parallel()
            .toDataset();
        
        System.out.println(joined);
    }
}
