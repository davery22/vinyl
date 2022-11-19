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

/**
 * Classes to support relational-style operations on streams of elements, such as select, aggregate, and join
 * transformations. The elements are represented as immutable "record" objects, which have their "fields" defined by a
 * relational operation, rather than a class declaration. For example:
 *
 * <pre>{@code
 *     Field<Long> userAge = new Field<>("userAge");
 *     Field<Long> userAccountAge = new Field<>("userAccountAge");
 *     Field<Long> countUsers = new Field<>("countUsers");
 *     Field<Double> avgAccountAge = new Field<>("avgAccountAge");
 *
 *     RecordSet data = RecordStream.aux(users.stream())
 *         .mapToRecord(into -> into
 *             .field(userAge, user -> ChronoUnit.YEARS.between(user.getBirthDate(), LocalDate.now()))
 *             .field(userAccountAge, user -> ChronoUnit.YEARS.between(user.getAccountStartDate(), LocalDate.now()))
 *         )
 *         .aggregate(aggregate -> aggregate
 *             .keyField(userAge)
 *             .aggField(countUsers, Collectors.counting())
 *             .aggField(avgAccountAge, Collectors.averagingLong(record -> record.get(userAccountAge)))
 *         )
 *         .toRecordSet();
 * }</pre>
 *
 * <p>Here we first define a few fields to use in subsequent relational operations, giving types for those fields. Then
 * we take a stream of {@code users}, transform it to a stream of records, and perform a grouped aggregation yielding a
 * new stream of records, which we finally collect into a {@code RecordSet}.
 *
 * <h2><a id="Records">Records, Headers, and Fields</a></h2>
 *
 * <p>{@code Record} is a central abstraction in Vinyl. A record is a shallowly-immutable carrier for a fixed set of
 * values, not unlike a {@code java.lang.Record} in Java 16+. However, Vinyl Records are not open to extension, and do
 * not declare any fields on the class. Instead, records are internally instantiated with a {@code Header} and a set of
 * values. The header defines what "fields" the record contains, and the values correspond to those fields. This allows
 * relational operations to dynamically define new combinations of fields for resulting records.
 *
 * <p>Each "field" in the record header is represented as a {@code Field}. A field is an opaque object with a
 * parameterized type - the type of values that a record may associate with that field. The field is used to access its
 * value on a record, similar to how normal fields are used to access values on an object. This ensures value access is
 * type-safe.
 *
 * <h2><a id="Aux">Auxiliary Streams</a></h2>
 *
 * <p>In the above "user aggregate" example, we first had to wrap the default {@code Stream} of users in a
 * {@code RecordStream.Aux}. This wrapper-stream extends the default stream with the {@code mapToRecord()} method, which
 * enables transformation into a {@code RecordStream}. Rather than have a static method to do this, using a
 * wrapper-stream allows for going back and forth between the stream types seamlessly. Other wrapper-stream types are
 * provided for each of the primitive stream types, ie {@code RecordStream.AuxInt}, {@code RecordStream.AuxLong}, and
 * {@code RecordStream.AuxDouble}. Existing stream operations that return a stream are overridden to return one of these
 * wrapper types.
 *
 * <h2><a id="RecordStreams">RecordStreams and RecordSets</a></h2>
 *
 * <p>A {@code RecordStream} is a stream of records sharing the same header. Record-streams extend normal streams with
 * relational operations like {@code select()}, {@code aggregate()}, and {@code join()}, which transform the header and
 * records in the stream.
 *
 * <p>A {@code RecordSet} stores records sharing the same header. Record-streams can be collected into record-sets,
 * allowing the records to be trivially re-streamed later, possibly multiple times.
 *
 * <h2><a id="Configurators">Configurators</a></h2>
 *
 * <p>Relational operations on a record-stream expose "configurators", which configure how the operation is executed.
 * For example, configurators are used to define the output fields of a {@code select()} operation, the join condition
 * of a {@code join()} operation, and the grouping keys of an {@code aggregate()} operation. Configurators may expose
 * sub-configurators for handling isolated specifics of the overall operation. To demonstrate:
 *
 * <pre>{@code
 *     RecordStream stream = scoresStream
 *         .select(select -> select
 *             .field(points)
 *             .window(window -> window
 *                 .field(averagePoints, Analytics.fromAgg(Collectors.averagingLong(points::get)))
 *                 .fields(Comparator.comparingLong(points::get), fields -> fields
 *                     .field(discreteMedian, Analytics.percentileDisc(0.5, points::get))
 *                     .field(continuousMedian, Analytics.percentileCont(0.5, points::get))
 *                 )
 *             )
 *         );
 * }</pre>
 *
 * <p>Here, the {@code select}, {@code window}, and {@code fields} configurators (named after the methods that introduce
 * them) are used to configure the overall {@code select()} operation. The {@code window} configurator is a
 * sub-configurator of {@code select}, and the {@code fields} configurator is a sub-configurator of {@code window}.
 * Together these configurators define four fields on the resultant record-stream: {@code points},
 * {@code averagePoints}, {@code discreteMedian}, {@code continuousMedian}.
 */
package vinyl;