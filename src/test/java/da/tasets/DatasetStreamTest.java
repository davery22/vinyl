package da.tasets;

import org.junit.jupiter.api.Test;

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
    
    @Test
    void test() {
        Dataset data = DatasetStream.aux(IntStream.range(0, 100)).boxed()
            .mapToDataset($->$
                .col(COL_A, i -> i + 1)
                .col(COL_B, i -> i + 2)
                .col(COL_D, i -> i + 3)
            )
            .select($->$
                .allExcept(COL_D)
                .col(COL_C, COL_D::get)
            )
            .aggregate($->$
                .keyCol(COL_A)
                .key(row -> row.get(COL_B) + row.get(COL_C))
                .aggCol(COL_C, summingInt(COL_A::get))
                .aggs(summarizingInt(COL_B::get), $$->$$
                    .aggCol(COL_D, IntSummaryStatistics::getMax)
                    .aggCol(COL_E, IntSummaryStatistics::getMin)
                    .aggCol(COL_F, IntSummaryStatistics::getSum)
                )
            )
            .toDataset();
        
        System.out.println(data.toString());
        
        System.out.println(data.stream()
            .collect(Dataset.collector($ -> data.header().selectAllExcept($, COL_D)))
            .toString()
        );
        
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
}
