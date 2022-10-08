package datasalad.util;

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
                .col(COL_A, it -> it + 1)
                .col(COL_B, it -> it + 2)
                .col(COL_D, it -> it + 3)
            )
            .select($->$
                .allExcept(COL_D)
                .col(COL_C, r -> r.get(COL_D))
            )
            .aggregate($->$
                .key(COL_A)
                .tempKey(COL_B, r -> r.get(COL_B) + r.get(COL_C))
                .agg(COL_C, summingInt(r -> r.get(COL_A)))
                .aggs(summarizingInt(r -> r.get(COL_B)), $$->$$
                    .agg(COL_D, IntSummaryStatistics::getMax)
                    .agg(COL_E, IntSummaryStatistics::getMin)
                    .agg(COL_F, IntSummaryStatistics::getSum)
                )
            )
            .toDataset();
        
        System.out.println(data.toString());
        
//        Dataset selfJoined = data.stream()
//            .join(data.stream(), $$->$$
//                .on($-> $.left(COL_A).eq($.right(COL_B)))
//                .andSelect($->$
//                    .lcol(COL_A)
//                    .rallExcept(COL_A)
//                )
//            )
//            .toDataset();
    }
}
