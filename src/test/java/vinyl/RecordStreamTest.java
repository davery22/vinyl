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

import java.util.*;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.summarizingInt;
import static java.util.stream.Collectors.summingInt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordStreamTest {
    private static final Field<Integer> A_INT = new Field<>("Aint");
    private static final Field<Integer> B_INT = new Field<>("Bint");
    private static final Field<Integer> C_INT = new Field<>("Cint");
    private static final Field<Integer> D_INT = new Field<>("Dint");
    private static final Field<Integer> E_INT = new Field<>("Eint");
    private static final Field<Long> A_LONG = new Field<>("Along");
    private static final Field<Long> B_LONG = new Field<>("Blong");
    private static final Field<Long> C_LONG = new Field<>("Clong");
    private static final Field<Long> D_LONG = new Field<>("Dlong");
    private static final Field<Double> A_DOUBLE = new Field<>("Adouble");
    private static final Field<Double> B_DOUBLE = new Field<>("Bdouble");
    private static final Field<Double> C_DOUBLE = new Field<>("Cdouble");
    private static final Field<String> A_STRING = new Field<>("Astring");
    
    @Test
    void testSimpleInnerJoin() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .select(select -> select
                .field(A_INT)
                .field(A_STRING, o -> "Hello " + o.get(A_INT))
            )
            .filter(o -> o.get(A_INT) > 2)
            .join(data.stream()
                      .select(select -> select
                          .field(A_INT)
                          .field(A_STRING, o -> "Goodbye " + o.get(A_INT))
                      )
                      .filter(o -> o.get(A_INT) < 8),
                  on -> on.eq(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(A_STRING, (l, r) -> l.get(A_STRING) + " and " + r.get(A_STRING))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, A_STRING },
            { 3, "Hello 3 and Goodbye 3" },
            { 4, "Hello 4 and Goodbye 4" },
            { 5, "Hello 5 and Goodbye 5" },
            { 6, "Hello 6 and Goodbye 6" },
            { 7, "Hello 7 and Goodbye 7" },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testSimpleLeftJoin() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .select(select -> select
                .field(A_INT)
                .field(A_STRING, o -> "Hello " + o.get(A_INT))
            )
            .filter(o -> o.get(A_INT) > 2)
            .leftJoin(data.stream()
                          .select(select -> select
                              .field(A_INT)
                              .field(A_STRING, o -> "Goodbye " + o.get(A_INT))
                          )
                          .filter(o -> o.get(A_INT) < 8),
                      on -> on.eq(on.left(A_INT), on.right(A_INT)),
                      select -> select
                          .leftField(A_INT)
                          .field(A_STRING, (l, r) -> l.get(A_STRING) + " and " + r.get(A_STRING))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, A_STRING },
            { 3, "Hello 3 and Goodbye 3" },
            { 4, "Hello 4 and Goodbye 4" },
            { 5, "Hello 5 and Goodbye 5" },
            { 6, "Hello 6 and Goodbye 6" },
            { 7, "Hello 7 and Goodbye 7" },
            { 8, "Hello 8 and null" },
            { 9, "Hello 9 and null" }
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testSimpleRightJoin() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .select(select -> select
                .field(A_INT)
                .field(A_STRING, o -> "Hello " + o.get(A_INT))
            )
            .filter(o -> o.get(A_INT) > 2)
            .rightJoin(data.stream()
                           .select(select -> select
                               .field(A_INT)
                               .field(A_STRING, o -> "Goodbye " + o.get(A_INT))
                           )
                           .filter(o -> o.get(A_INT) < 8),
                       on -> on.eq(on.left(A_INT), on.right(A_INT)),
                       select -> select
                           .rightField(A_INT)
                           .field(A_STRING, (l, r) -> l.get(A_STRING) + " and " + r.get(A_STRING))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, A_STRING },
            { 0, "null and Goodbye 0" },
            { 1, "null and Goodbye 1" },
            { 2, "null and Goodbye 2" },
            { 3, "Hello 3 and Goodbye 3" },
            { 4, "Hello 4 and Goodbye 4" },
            { 5, "Hello 5 and Goodbye 5" },
            { 6, "Hello 6 and Goodbye 6" },
            { 7, "Hello 7 and Goodbye 7" },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testSimpleFullJoin() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .select(select -> select
                .field(A_INT)
                .field(A_STRING, o -> "Hello " + o.get(A_INT))
            )
            .filter(o -> o.get(A_INT) > 2)
            .fullJoin(data.stream()
                          .select(select -> select
                              .field(A_INT)
                              .field(A_STRING, o -> "Goodbye " + o.get(A_INT))
                          )
                          .filter(o -> o.get(A_INT) < 8),
                      on -> on.eq(on.left(A_INT), on.right(A_INT)),
                      select -> select
                          .field(A_INT, (l, r) -> l.isNil() ? r.get(A_INT) : l.get(A_INT))
                          .field(A_STRING, (l, r) -> l.get(A_STRING) + " and " + r.get(A_STRING))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, A_STRING },
            { 0, "null and Goodbye 0" },
            { 1, "null and Goodbye 1" },
            { 2, "null and Goodbye 2" },
            { 3, "Hello 3 and Goodbye 3" },
            { 4, "Hello 4 and Goodbye 4" },
            { 5, "Hello 5 and Goodbye 5" },
            { 6, "Hello 6 and Goodbye 6" },
            { 7, "Hello 7 and Goodbye 7" },
            { 8, "Hello 8 and null" },
            { 9, "Hello 9 and null" }
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinOnNEQ() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .join(data.stream(),
                  on -> on.neq(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 10).boxed()
                    .flatMap(i -> IntStream.range(0, 10).filter(j -> j != i).mapToObj(j -> new Object[]{ i, j }))
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinOnCEQ() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .join(data.stream(),
                  on -> on.ceq(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 10).boxed().map(i -> new Object[]{ i, i })
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinOnCNEQ() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .join(data.stream(),
                  on -> on.cneq(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 10).boxed()
                    .flatMap(i -> IntStream.range(0, 10).filter(j -> j != i).boxed().map(j -> new Object[]{ i, j }))
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinOnGT() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .join(data.stream(),
                  on -> on.gt(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 10).boxed()
                    .flatMap(i -> IntStream.range(0, i).boxed().map(j -> new Object[]{ i, j }))
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinOnGTE() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .join(data.stream(),
                  on -> on.gte(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 10).boxed()
                    .flatMap(i -> IntStream.range(0, i+1).boxed().map(j -> new Object[]{ i, j }))
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinOnLT() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .join(data.stream(),
                  on -> on.lt(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 10).boxed()
                    .flatMap(i -> IntStream.range(i+1, 10).boxed().map(j -> new Object[]{ i, j }))
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinOnLTE() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .join(data.stream(),
                  on -> on.lte(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 10).boxed()
                    .flatMap(i -> IntStream.range(i, 10).boxed().map(j -> new Object[]{ i, j }))
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition1() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 5)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .parallel()
            .rightJoin(data.stream(),
                  on -> on.any(
                      on.between(on.left(A_INT), on.right(o -> o.get(A_INT)-1), on.val(3)),
                      on.all(
                          on.leftMatch(o -> o.get(A_INT) == 4),
                          on.rightMatch(o -> o.get(A_INT) == 4)
                      )
                  ),
                  select -> select
                      .rightField(A_INT)
                      .field(B_INT, (l, r) -> l.get(A_INT))
            )
            .sorted()
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT },
            { 0, 0 },
            { 0, 1 },
            { 0, 2 },
            { 0, 3 },
            { 1, 0 },
            { 1, 1 },
            { 1, 2 },
            { 1, 3 },
            { 2, 1 },
            { 2, 2 },
            { 2, 3 },
            { 3, 2 },
            { 3, 3 },
            { 4, 3 },
            { 4, 4 },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition2() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 5)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .parallel()
            .join(data.stream(),
                  on -> on.not(on.match((l, r) -> Objects.equals(l.get(A_INT), r.get(A_INT)))),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(
            Stream.concat(
                Stream.<Object[]>of(new Object[]{ A_INT, B_INT }),
                IntStream.range(0, 5).boxed()
                    .flatMap(i -> IntStream.range(0, 5).filter(j -> j != i).mapToObj(j -> new Object[]{ i, j }))
            ).toArray(Object[][]::new)
        );
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition3() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 5)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .parallel()
            .leftJoin(data.stream(),
                  on -> on.lt(on.left(o -> o.get(A_INT) + 5), on.eval(() -> 7)),
                  select -> select
                      .leftField(A_INT)
                      .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT },
            { 0, 0 },
            { 0, 1 },
            { 0, 2 },
            { 0, 3 },
            { 0, 4 },
            { 1, 0 },
            { 1, 1 },
            { 1, 2 },
            { 1, 3 },
            { 1, 4 },
            { 2, null },
            { 3, null },
            { 4, null },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition4() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 5)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .parallel()
            .leftJoin(data.stream(),
                      on -> on.lt(on.val(3), on.val(2)),
                      select -> select
                          .leftField(A_INT)
                          .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT },
            { 0, null },
            { 1, null },
            { 2, null },
            { 3, null },
            { 4, null }
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition5() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 5)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .parallel()
            .leftJoin(data.stream(),
                      on -> on.lt(on.left(A_INT), on.left(o -> 2)),
                      select -> select
                          .leftField(A_INT)
                          .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT },
            { 0, 0 },
            { 0, 1 },
            { 0, 2 },
            { 0, 3 },
            { 0, 4 },
            { 1, 0 },
            { 1, 1 },
            { 1, 2 },
            { 1, 3 },
            { 1, 4 },
            { 2, null },
            { 3, null },
            { 4, null },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition6() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 5)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .parallel()
            .rightJoin(data.stream(),
                       on -> on.lt(on.val(3), on.right(A_INT)),
                       select -> select
                           .leftField(A_INT)
                           .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT },
            { null, 0 },
            { null, 1 },
            { null, 2 },
            { null, 3 },
            { 0, 4 },
            { 1, 4 },
            { 2, 4 },
            { 3, 4 },
            { 4, 4 },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition7() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 5)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
        
        RecordSet joined = data.stream()
            .parallel()
            .rightJoin(data.stream(),
                       on -> on.lt(on.right(o -> 3), on.right(A_INT)),
                       select -> select
                           .leftField(A_INT)
                           .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT },
            { null, 0 },
            { null, 1 },
            { null, 2 },
            { null, 3 },
            { 0, 4 },
            { 1, 4 },
            { 2, 4 },
            { 3, 4 },
            { 4, 4 },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinCondition8() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 3)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .toRecordSet();
    
        RecordSet joined = data.stream()
            .parallel()
            .fullJoin(data.stream(),
                      on -> on.eq(on.val(1), on.val(1)),
                      select -> select
                          .leftField(A_INT)
                          .field(B_INT, (l, r) -> r.get(A_INT))
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT },
            { 0, 0 },
            { 0, 1 },
            { 0, 2 },
            { 1, 0 },
            { 1, 1 },
            { 1, 2 },
            { 2, 0 },
            { 2, 1 },
            { 2, 2 },
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinSelect1() {
        RecordSet joined = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i+1)
                .field(C_INT, i -> i+2)
            )
            .join(RecordStream.aux(IntStream.range(0, 10)).boxed()
                      .mapToRecord(into -> into
                          .field(A_INT, i -> i)
                          .field(A_LONG, i -> i-1L)
                          .field(B_LONG, i -> i-2L)
                      ),
                  on -> on.eq(on.left(A_INT), on.right(A_INT)),
                  select -> select.leftAllFields().rightAllFields()
            )
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT, C_INT, A_LONG, B_LONG },
            { 0,  1,  2, -1L, -2L },
            { 1,  2,  3,  0L, -1L },
            { 2,  3,  4,  1L,  0L },
            { 3,  4,  5,  2L,  1L },
            { 4,  5,  6,  3L,  2L },
            { 5,  6,  7,  4L,  3L },
            { 6,  7,  8,  5L,  4L },
            { 7,  8,  9,  6L,  5L },
            { 8,  9, 10,  7L,  6L },
            { 9, 10, 11,  8L,  7L }
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinSelect2() {
        RecordSet joined = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i+1)
                .field(C_INT, i -> i+2)
            )
            .join(RecordStream.aux(IntStream.range(0, 10)).boxed()
                      .mapToRecord(into -> into
                          .field(A_INT, i -> i)
                          .field(A_LONG, i -> i-1L)
                          .field(B_LONG, i -> i-2L)
                      ),
                  on -> on.eq(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftAllFieldsExcept(B_INT)
                      .rightAllFieldsExcept(A_LONG)
                      .fields((l, r) -> l.get(C_INT) + r.get(B_LONG), fields -> fields
                          .field(B_INT, i -> i.intValue()-10)
                          .field(A_LONG, i -> i+10)
                      )
            )
            .toRecordSet();
    
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, C_INT, B_LONG, B_INT, A_LONG },
            { 0,  2, -2L, -10, 10L },
            { 1,  3, -1L,  -8, 12L },
            { 2,  4,  0L,  -6, 14L },
            { 3,  5,  1L,  -4, 16L },
            { 4,  6,  2L,  -2, 18L },
            { 5,  7,  3L,   0, 20L },
            { 6,  8,  4L,   2, 22L },
            { 7,  9,  5L,   4, 24L },
            { 8, 10,  6L,   6, 26L },
            { 9, 11,  7L,   8, 28L }
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testJoinSelect3() {
        RecordSet joined = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i+1)
                .field(C_INT, i -> i+2)
            )
            .join(RecordStream.aux(IntStream.range(0, 10)).boxed()
                      .mapToRecord(into -> into
                          .field(A_INT, i -> i)
                          .field(A_LONG, i -> i-1L)
                          .field(B_LONG, i -> i-2L)
                      ),
                  on -> on.eq(on.left(A_INT), on.right(A_INT)),
                  select -> select
                      .leftFields(A_INT, B_INT)
                      .rightFields(A_LONG, B_LONG)
            )
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT, A_LONG, B_LONG },
            { 0,  1, -1L, -2L },
            { 1,  2,  0L, -1L },
            { 2,  3,  1L,  0L },
            { 3,  4,  2L,  1L },
            { 4,  5,  3L,  2L },
            { 5,  6,  4L,  3L },
            { 6,  7,  5L,  4L },
            { 7,  8,  6L,  5L },
            { 8,  9,  7L,  6L },
            { 9, 10,  8L,  7L }
        });
        assertEquals(expected, joined);
    }
    
    @Test
    void testTransformRecordStreamToRecordSet() {
        RecordSet set = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i*2)
            )
            .toRecordSet();
        
        List<Integer> expected = IntStream.range(0, 100).map(i -> i*3).boxed().collect(Collectors.toList());
        List<Integer> actual = set.stream().map(o -> o.get(A_INT) + o.get(B_INT)).collect(Collectors.toList());
        
        assertEquals(List.of(A_INT, B_INT), set.header().fields());
        assertEquals(expected, actual);
    }
    
    @Test
    void testTransformEmptyRecordStreamToRecordSet() {
        RecordSet set = RecordStream.aux(Stream.empty())
            .mapToRecord(into -> into
                .field(A_INT, i -> 1)
                .field(B_INT, i -> 2)
            )
            .toRecordSet();
    
        assertEquals(List.of(A_INT, B_INT), set.header().fields());
        assertEquals(List.of(), set.stream().collect(Collectors.toList()));
    }
    
    @Test
    void testCollectRecordStreamToRecordSet() {
        RecordSet set = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .collect(RecordSet.collector(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i*2)
            ));
        
        List<Integer> expected = IntStream.range(0, 100).map(i -> i*3).boxed().collect(Collectors.toList());
        List<Integer> actual = set.stream().map(o -> o.get(A_INT) + o.get(B_INT)).collect(Collectors.toList());
    
        assertEquals(List.of(A_INT, B_INT), set.header().fields());
        assertEquals(expected, actual);
    }
    
    @Test
    void testParallelCollectRecordStreamToRecordSet() {
        RecordSet set = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .parallel()
            .collect(RecordSet.collector(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i*2)
            ));
        
        List<Integer> expected = IntStream.range(0, 100).map(i -> i*3).boxed().collect(Collectors.toList());
        List<Integer> actual = set.stream().map(o -> o.get(A_INT) + o.get(B_INT)).collect(Collectors.toList());
        
        assertEquals(List.of(A_INT, B_INT), set.header().fields());
        assertEquals(expected, actual);
    }
    
    @Test
    void testCollectEmptyRecordStreamToRecordSet() {
        RecordSet set = RecordStream.aux(Stream.empty())
            .collect(RecordSet.collector(into -> into
                .field(A_INT, i -> 1)
                .field(B_INT, i -> 2)
            ));
    
        assertEquals(List.of(A_INT, B_INT), set.header().fields());
        assertEquals(List.of(), set.stream().collect(Collectors.toList()));
    }
    
    @Test
    void testAnalytics() {
        Comparator<Record> comparator = Comparator.comparingInt(A_INT::get);
        RecordSet data = RecordStream.aux(Stream.of(1, 2, 3, 2, 4, 8, 4, 4, 5, 9))
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
            )
            .select(select -> select
                .field(A_INT)
                .window(window -> window
                    .field(B_INT, Analytics.fromAgg(Collectors.summingInt(A_INT::get)))
                    .fields(comparator, fields -> fields
                        .field(A_LONG, Analytics.rowNumber(0))
                        .field(B_LONG, Analytics.rank(0, comparator))
                        .field(C_LONG, Analytics.denseRank(0, comparator))
                        .field(D_LONG, Analytics.nTile(0, 4))
                        .field(C_INT, Analytics.link(A_INT::get, 1, -1))
                        .field(A_DOUBLE, Analytics.cumeDist(comparator))
                        .field(B_DOUBLE, Analytics.percentRank(comparator))
                        .field(D_INT, Analytics.percentileDisc(0.7, A_INT::get))
                        .field(C_DOUBLE, Analytics.percentileCont(0.7, A_INT::get))
                    )
                )
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT, A_LONG, B_LONG, C_LONG, D_LONG, C_INT, A_DOUBLE, B_DOUBLE, D_INT, C_DOUBLE },
            { 1, 42, 0L, 0L, 0L, 0L,  2, 1d/10,   0d, 4, 4.3d },
            { 2, 42, 1L, 1L, 1L, 0L,  2, 3d/10, 1d/9, 4, 4.3d },
            { 2, 42, 2L, 1L, 1L, 0L,  3, 3d/10, 1d/9, 4, 4.3d },
            { 3, 42, 3L, 3L, 2L, 1L,  4, 4d/10, 3d/9, 4, 4.3d },
            { 4, 42, 4L, 4L, 3L, 1L,  4, 7d/10, 4d/9, 4, 4.3d },
            { 4, 42, 5L, 4L, 3L, 1L,  4, 7d/10, 4d/9, 4, 4.3d },
            { 4, 42, 6L, 4L, 3L, 2L,  5, 7d/10, 4d/9, 4, 4.3d },
            { 5, 42, 7L, 7L, 4L, 2L,  8, 8d/10, 7d/9, 4, 4.3d },
            { 8, 42, 8L, 8L, 5L, 3L,  9, 9d/10, 8d/9, 4, 4.3d },
            { 9, 42, 9L, 9L, 6L, 3L, -1,    1d,   1d, 4, 4.3d },
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testRecordSetToString() {
        RecordSet set = unsafeRecordSet(new Object[][]{
            { A_INT, A_LONG, A_STRING },
            {  0, 22L, "some" },
            { 10,  7L, "test" },
            { 11,  3L, "data" },
        });
        String expected =
            "RecordSet[\n"
            + "\t[Aint, Along, Astring],\n"
            + "\t[0, 22, some],\n"
            + "\t[10, 7, test],\n"
            + "\t[11, 3, data]\n"
            + "]";
        assertEquals(expected, set.toString());
    }
    
    @Test
    void testEmptyRecordSetToString() {
        RecordSet set = RecordStream.aux(Stream.empty()).mapToRecord(into -> {}).toRecordSet();
        String expected =
            "RecordSet[\n"
            + "\t[]\n"
            + "]";
        assertEquals(expected, set.toString());
    }
    
    @Test
    void testRecordToString() {
        Record record = unsafeRecordSet(new Object[][]{
            { A_INT, A_LONG, A_STRING },
            { 2040, 792L, "hello" }
        }).stream().findFirst().get();
        String expected = "Record{Aint=2040, Along=792, Astring=hello}";
        assertEquals(expected, record.toString());
    }
    
    @Test
    void testHeaderToString() {
        Header header = unsafeRecordSet(new Object[][]{
            { A_INT, A_LONG, A_STRING }
        }).header();
        String expected = "Header[Aint, Along, Astring]";
        assertEquals(expected, header.toString());
    }
    
    @Test
    void testFieldPinToString() {
        FieldPin<Integer> pinA = new FieldPin<>(A_INT, 2);
        assertEquals("Aint @ 2", pinA.toString());
    }
    
    @Test
    void testSelect1() {
        RecordSet data = RecordStream.aux(IntStream.range(45, 55)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i+1)
                .field(D_INT, i -> i+3)
            )
            .select(select -> select
                .allFields()
                .field(C_INT, o -> o.get(A_INT) + 2)
                .field(A_INT, o -> o.get(A_INT) + 1) // overrides definition of A_INT from allFields()
                .fields(o -> o.get(A_INT) % 10, fields -> fields
                    .field(A_LONG, i -> i+10L)
                    .field(B_LONG, i -> i+11L)
                )
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT, D_INT, C_INT, A_LONG, B_LONG },
            { 46, 46, 48, 47, 15L, 16L },
            { 47, 47, 49, 48, 16L, 17L },
            { 48, 48, 50, 49, 17L, 18L },
            { 49, 49, 51, 50, 18L, 19L },
            { 50, 50, 52, 51, 19L, 20L },
            { 51, 51, 53, 52, 10L, 11L },
            { 52, 52, 54, 53, 11L, 12L },
            { 53, 53, 55, 54, 12L, 13L },
            { 54, 54, 56, 55, 13L, 14L },
            { 55, 55, 57, 56, 14L, 15L }
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testSelect2() {
        RecordSet data = RecordStream.aux(IntStream.range(46, 56)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(B_INT, i -> i+1)
                .field(C_INT, i -> i+2)
                .field(D_INT, i -> i/10)
            )
            .select(select -> select
                .fields(A_INT, B_INT)
                .window(window -> window
                    .keys(D_INT)
                    .keys(o -> o.get(A_INT)/10, keys -> keys
                        .key(i -> i)
                    )
                    .fields(List::size, fields -> fields
                        .field(C_INT, (size, sink) -> sink.accept(size))
                        .field(D_INT, (size, sink) -> sink.accept(size+1))
                    )
                )
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT, C_INT, D_INT },
            { 46, 47, 4, 5 },
            { 47, 48, 4, 5 },
            { 48, 49, 4, 5 },
            { 49, 50, 4, 5 },
            { 50, 51, 6, 7 },
            { 51, 52, 6, 7 },
            { 52, 53, 6, 7 },
            { 53, 54, 6, 7 },
            { 54, 55, 6, 7 },
            { 55, 56, 6, 7 }
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testSelect3() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i*2))
            .select(select -> select
                .field(A_INT)
                .window(window -> window
                    .field(B_INT, Analytics.link(A_INT::get, -1, 404))
                    .field(A_LONG, Comparator.comparingInt(A_INT::get).reversed(), Analytics.rowNumber(1))
                    .fields(Comparator.comparingInt(A_INT::get), fields -> fields
                        .field(B_LONG, Analytics.percentileDisc(0.5, o -> o.get(A_INT).longValue()))
                        .field(C_LONG, Analytics.percentileDisc(0.50000001, o -> o.get(A_INT).longValue()))
                        .field(A_DOUBLE, Analytics.percentileCont(0.5, A_INT::get))
                    )
                    .field(C_INT, (o, sink) -> sink.accept(o.stream().mapToInt(A_INT::get).sum()))
                )
                .window(window -> window
                    .key(o -> o.get(A_INT) / 10)
                    .field(D_INT, (o, sink) -> sink.accept(o.stream().mapToInt(A_INT::get).max().getAsInt()))
                )
            )
            .sorted(Comparator.comparingInt(A_INT::get).reversed())
            .parallel()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT, A_LONG, B_LONG, C_LONG, A_DOUBLE, C_INT, D_INT },
            { 18,  16,  1L, 8L, 10L, 9.0d, 90, 18 },
            { 16,  14,  2L, 8L, 10L, 9.0d, 90, 18 },
            { 14,  12,  3L, 8L, 10L, 9.0d, 90, 18 },
            { 12,  10,  4L, 8L, 10L, 9.0d, 90, 18 },
            { 10,   8,  5L, 8L, 10L, 9.0d, 90, 18 },
            {  8,   6,  6L, 8L, 10L, 9.0d, 90,  8 },
            {  6,   4,  7L, 8L, 10L, 9.0d, 90,  8 },
            {  4,   2,  8L, 8L, 10L, 9.0d, 90,  8 },
            {  2,   0,  9L, 8L, 10L, 9.0d, 90,  8 },
            {  0, 404, 10L, 8L, 10L, 9.0d, 90,  8 },
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testMisbehavingAnalyticFunction1() {
        assertThrows(IllegalStateException.class, () -> {
            RecordStream.aux(IntStream.range(0, 10)).boxed()
                .mapToRecord(into -> into.field(A_INT, i -> i))
                .select(select -> select
                    .window(window -> window
                        .field(A_INT, (list, sink) -> {
                            // Emits list.size()-1 times
                            for (int i = 1; i < list.size(); i++)
                                sink.accept(1);
                        })
                    )
                )
                .toRecordSet();
        });
    }
    
    @Test
    void testMisbehavingAnalyticFunction2() {
        assertThrows(IllegalStateException.class, () -> {
            RecordStream.aux(IntStream.range(0, 10)).boxed()
                .mapToRecord(into -> into.field(A_INT, i -> i))
                .select(select -> select
                    .window(window -> window
                        .field(A_INT, (list, sink) -> {
                            // Emits list.size()+1 times
                            for (Record ignored : list)
                                sink.accept(1);
                            sink.accept(1);
                        })
                    )
                )
                .toRecordSet();
        });
    }
    
    @Test
    void testMisbehavingAnalyticFunction3() {
        assertThrows(UnsupportedOperationException.class, () -> {
            RecordStream.aux(IntStream.range(0, 10)).boxed()
                .mapToRecord(into -> into.field(A_INT, i -> i))
                .select(select -> select
                    .window(window -> window
                        .field(A_INT, (list, sink) -> {
                            // Attempts to modify the list
                            list.set(0, list.get(1));
                            sink.accept(1);
                        })
                    )
                )
                .toRecordSet();
        });
    }
    
    @Test
    void testAggregate1() {
        Field<Map<Integer, Double>> avg = new Field<>("avg");
        RecordSet data = RecordStream.aux(IntStream.range(0, 1000)).boxed()
            .parallel()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .aggregate(aggregate -> aggregate
                .aggs(Collectors.summarizingLong(A_INT::get), aggs -> aggs
                    .aggField(A_LONG, LongSummaryStatistics::getMax)
                    .aggField(B_LONG, LongSummaryStatistics::getMin)
                    .aggField(C_LONG, LongSummaryStatistics::getCount)
                    .aggField(D_LONG, LongSummaryStatistics::getSum)
                    .aggField(A_DOUBLE, LongSummaryStatistics::getAverage)
                )
                .aggField(avg, Collectors.groupingBy(o -> o.get(A_INT)/100, Collectors.averagingInt(A_INT::get)))
            )
            .toRecordSet();
        
        Map<Integer, Double> expectedAverages = Map.ofEntries(
            Map.entry(0, 4950d/100),
            Map.entry(1, 14950d/100),
            Map.entry(2, 24950d/100),
            Map.entry(3, 34950d/100),
            Map.entry(4, 44950d/100),
            Map.entry(5, 54950d/100),
            Map.entry(6, 64950d/100),
            Map.entry(7, 74950d/100),
            Map.entry(8, 84950d/100),
            Map.entry(9, 94950d/100)
        );
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_LONG, B_LONG, C_LONG, D_LONG, A_DOUBLE, avg },
            { 999L, 0L, 1000L, 499500L, 499500d/1000, expectedAverages }
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testAggregate2() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 1000)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(C_INT, i -> i+2)
                .field(D_INT, i -> i/100)
            )
            .aggregate(aggregate -> aggregate
                .keys(D_INT)
                .keys(o -> o.get(A_INT)/100, keys -> keys
                    .key(i -> i)
                )
                .aggField(A_LONG, Collectors.summingLong(A_INT::get))
                .aggField(B_LONG, Collectors.counting())
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_LONG, B_LONG },
            {  4950L, 100L },
            { 14950L, 100L },
            { 24950L, 100L },
            { 34950L, 100L },
            { 44950L, 100L },
            { 54950L, 100L },
            { 64950L, 100L },
            { 74950L, 100L },
            { 84950L, 100L },
            { 94950L, 100L },
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testAggregate3() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 1000)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i)
                .field(C_INT, i -> i+2)
                .field(D_INT, i -> i/100)
            )
            .aggregate(aggregate -> aggregate
                .key(D_INT::get)
                .keyFields(D_INT)
                .keys(o -> o.get(A_INT)/100, keys -> keys
                    .keyField(C_INT, i -> i)
                )
                .aggField(A_LONG, Collectors.summingLong(A_INT::get))
                .aggField(B_LONG, Collectors.counting())
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { D_INT, C_INT, A_LONG, B_LONG },
            { 0, 0,  4950L, 100L },
            { 1, 1, 14950L, 100L },
            { 2, 2, 24950L, 100L },
            { 3, 3, 34950L, 100L },
            { 4, 4, 44950L, 100L },
            { 5, 5, 54950L, 100L },
            { 6, 6, 64950L, 100L },
            { 7, 7, 74950L, 100L },
            { 8, 8, 84950L, 100L },
            { 9, 9, 94950L, 100L },
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testAggregate4() {
        RecordStream stream = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .mapToRecord(into -> into
                .field(A_INT, i -> i + 1)
                .field(B_INT, i -> i + 2)
                .field(D_INT, i -> i + 3)
            )
            .select(select -> select
                .allFieldsExcept(D_INT)
                .field(C_INT, D_INT::get)
            );
        FieldPin<Integer> pinB = stream.header().pin(B_INT);
        FieldPin<Integer> pinC = stream.header().pin(C_INT);
        RecordSet data = stream
            .aggregate(aggregate -> aggregate
                .keyField(A_INT, o -> (o.get(pinB) + o.get(pinC)) % 10)
                .aggField(C_INT, summingInt(A_INT::get))
                .aggs(summarizingInt(B_INT::get), aggs -> aggs
                    .aggField(D_INT, IntSummaryStatistics::getMax)
                    .aggField(E_INT, IntSummaryStatistics::getMin)
                    .aggField(A_LONG, IntSummaryStatistics::getSum)
                )
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, C_INT, D_INT, E_INT, A_LONG },
            { 1, 1030, 100, 5, 1050L },
            { 3, 1050, 101, 6, 1070L },
            { 5,  970,  97, 2,  990L },
            { 7,  990,  98, 3, 1010L },
            { 9, 1010,  99, 4, 1030L },
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testAggregateRoute1() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 100)).boxed()
            .parallel()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .aggregate(aggregate -> aggregate
                .keyField(A_INT, o -> o.get(A_INT)%10)
                .<Long>route(
                    (record, sink) -> {
                        sink.accept(A_INT, 3L); // not a child - ignored
                        sink.accept(B_LONG, (long) record.get(A_INT));
                        sink.accept(C_LONG, 1L);
                        sink.accept(C_LONG, 1L); // ok to send multiple values to same field
                    },
                    route -> route
                        .aggField(B_LONG, Collectors.summingLong(i -> i))
                        .aggField(C_LONG, Collectors.summingLong(i -> i))
                        .aggField(D_LONG, Collectors.counting()) // nothing emitted to this field
                )
            )
            .sorted()
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_LONG, C_LONG, D_LONG },
            { 0, 450L, 20L, 0L },
            { 1, 460L, 20L, 0L },
            { 2, 470L, 20L, 0L },
            { 3, 480L, 20L, 0L },
            { 4, 490L, 20L, 0L },
            { 5, 500L, 20L, 0L },
            { 6, 510L, 20L, 0L },
            { 7, 520L, 20L, 0L },
            { 8, 530L, 20L, 0L },
            { 9, 540L, 20L, 0L }
        });
        assertEquals(expected, data);
    }
    
    @Test
    void testSelectPostFields() {
        RecordSet data = RecordStream.aux(IntStream.range(0, 10)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .select(select -> select
                .field(A_INT)
                .postField(B_INT, o -> o.get(A_INT) + o.get(C_INT))
                .postField(C_INT, o -> o.get(A_INT) + o.get(D_INT))
                .postField(D_INT, o -> 1)
            )
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { A_INT, B_INT, C_INT, D_INT },
            { 0,  1,  1, 1 },
            { 1,  3,  2, 1 },
            { 2,  5,  3, 1 },
            { 3,  7,  4, 1 },
            { 4,  9,  5, 1 },
            { 5, 11,  6, 1 },
            { 6, 13,  7, 1 },
            { 7, 15,  8, 1 },
            { 8, 17,  9, 1 },
            { 9, 19, 10, 1 },
        });
        assertEquals(expected, data);
    }
    
    // Constructors are not strictly necessary, but are used to disambiguate field names.
    private static class TableRef {
        final RowRef row1;
        final RowRef row2;
        final RowRef row3;
        
        TableRef(String id) {
            this.row1 = new RowRef(id + ".row1");
            this.row2 = new RowRef(id + ".row2");
            this.row3 = new RowRef(id + ".row3");
        }
        
        private static class RowRef {
            final Field<String> name;
            final Field<Long> balance1;
            final Field<Long> balance2;
            final Field<Long> balance3;
            
            RowRef(String id) {
                this.name = new Field<>(id + ".name");
                this.balance1 = new Field<>(id + ".balance1");
                this.balance2 = new Field<>(id + ".balance2");
                this.balance3 = new Field<>(id + ".balance3");
            }
        }
    }
    
    // This could be generated from TableRef using annotation processing.
    private static class Table {
        final Row row1;
        final Row row2;
        final Row row3;
        
        Table(Row row1, Row row2, Row row3) {
            this.row1 = row1;
            this.row2 = row2;
            this.row3 = row3;
        }
        
        static Function<Record, Table> creator(TableRef ref) {
            return record ->
                new Table(Row.creator(ref.row1).apply(record),
                          Row.creator(ref.row2).apply(record),
                          Row.creator(ref.row3).apply(record)
                );
        }
        
        private static class Row {
            final String name;
            final long balance1;
            final long balance2;
            final long balance3;
            
            Row(String name, long balance1, long balance2, long balance3) {
                this.name = name;
                this.balance1 = balance1;
                this.balance2 = balance2;
                this.balance3 = balance3;
            }
            
            static Function<Record, Table.Row> creator(TableRef.RowRef ref) {
                return record ->
                    new Table.Row(record.get(ref.name),
                                  record.get(ref.balance1),
                                  record.get(ref.balance2),
                                  record.get(ref.balance3)
                    );
            }
        }
    }
    
    private static void row(TableRef.RowRef row,
                            String name,
                            AggregateAPI.Route<Long> route) {
        route.parent().postField(row.name, o -> name);
        route.aggField(row.balance1, Collectors.summingLong(i -> i));
        route.aggField(row.balance2, Collectors.summingLong(i -> i));
        route.parent().postField(row.balance3, o -> o.get(row.balance2) - o.get(row.balance1));
    }
    
    private static void totalRow(TableRef.RowRef row,
                                 String name,
                                 AggregateAPI.Route<Long> route,
                                 ToLongBiFunction<Record, Function<TableRef.RowRef, Field<Long>>> mapper) {
        row(row, name, route);
        route.parent().postField(row.balance1, o -> mapper.applyAsLong(o, r -> r.balance1));
        route.parent().postField(row.balance2, o -> mapper.applyAsLong(o, r -> r.balance2));
    }
    
    @Test
    void testAdvancedAggregate() {
        // This transformation conceptually goes through 3 phases:
        //
        // -- input table of integers --
        // [A]
        // [0]
        // [1]
        // [2]
        // [3]
        // ...
        // [999]
        //
        // -- intermediate wide table --
        // [row1.name, row1.balance1, row1.balance2, row1.balance3, row2.name, row2.balance1, ...]
        // [foo, 234, 234, 643, bar, 839, 583, ...]
        //
        // -- output table --
        // [name, balance1, balance2, balance3]
        // [foo, 234, 234, 643]
        // [bar, 839, 583, 234]
        // [baz, 2342, 234, 12]
        
        TableRef.RowRef row = new TableRef.RowRef("row");
        TableRef table = new TableRef("table");
    
        RecordSet data = RecordStream.aux(IntStream.range(0, 1000)).boxed()
            .mapToRecord(into -> into.field(A_INT, i -> i))
            .aggregate(aggregate -> aggregate
                .<Long>route(
                    (record, sink) -> {
                        long a = record.get(A_INT);
                        if (a % 2 == 0) {
                            sink.accept(table.row1.balance1, a);
                            sink.accept(table.row1.balance2, a+1);
                        } else {
                            sink.accept(table.row2.balance1, a);
                            sink.accept(table.row2.balance2, a+1);
                        }
                    },
                    route -> {
                        row(table.row1, "first", route);
                        row(table.row2, "second", route);
                        totalRow(table.row3, "third", route, (o, col) ->
                              o.get(col.apply(table.row2))
                            - o.get(col.apply(table.row1))
                        );
                    }
                )
            ) // Now a single flattened record
            .map(Table.creator(table)) // Now a custom representation
            .flatMap(tab -> Stream.of(tab.row1, tab.row2, tab.row3))
            .mapToRecord(into -> into
                .field(row.name, o -> o.name)
                .field(row.balance1, o -> o.balance1)
                .field(row.balance2, o -> o.balance2)
                .field(row.balance3, o -> o.balance3)
            ) // Now a record-per-row
            .toRecordSet();
        
        RecordSet expected = unsafeRecordSet(new Object[][]{
            { row.name, row.balance1, row.balance2, row.balance3 },
            {  "first", 249500L, 250000L,  500L },
            { "second", 250000L, 250500L,  500L },
            {  "third",    500L,    500L,    0L },
        });
        assertEquals(expected, data);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private RecordSet unsafeRecordSet(Object[][] arr) {
        Object[] fields = arr[0];
        return RecordStream.aux(Arrays.stream(arr, 1, arr.length))
            .mapToRecord(into -> {
                for (int i = 0; i < fields.length; i++) {
                    int j = i;
                    into.field((Field) fields[j], a -> a[j]);
                }
            })
            .toRecordSet();
    }
}
