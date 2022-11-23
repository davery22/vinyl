# Vinyl: Relational Streams for Java

Vinyl is now in beta. Feedback is welcome!

Inspired by SQL, built for Java. Vinyl extends Java Streams with relational operations, based around its central Record
type. It aims to integrate smoothly and minimally on top of existing Streams, while staying efficient, safe, and
easy-to-use.

Vinyl requires a Java version of at least 9.

## Adding Vinyl to your build

Vinyl's Maven group ID is `io.avery`, and its artifact ID is `vinyl`.

To add a dependency on Vinyl using Maven, use the following:

```xml
<dependency>
  <groupId>io.avery</groupId>
  <artifactid>vinyl</artifactid>
  <version>0.1</version>
</dependency>
```

## A Guided Example

To make use of Vinyl, we first need to declare some "fields" that we will use later in relational operations.

```java
Field<Integer> number = new Field<>("number");
Field<Integer> times2 = new Field<>("times2");
Field<Integer> square = new Field<>("square");
```

Notice that we give each field a name and a type argument. The name is the field's `toString()` representation. The type
argument says what type of values a "record" can associate with this field.

Now we can start writing streams. For a simple example, we'll enrich a sequence of numbers:

```java
RecordStream numbers = RecordStream.aux(IntStream.range(0, 10).boxed())
    .mapToRecord(into -> into
        .field(number, i -> i)
        .field(times2, i -> i + i)
        .field(square, i -> i * i)
    );
```

We first wrap a normal stream in a `RecordStream.Aux`, an "auxiliary" stream that extends `Stream` with the
`mapToRecord()` method. With `mapToRecord()`, we describe how to create each field on an outgoing record, from an 
incoming element. The API ensures that all outgoing records share the same set of fields, or "header". This is a key
aspect of a `RecordStream`.

At this point, the data conceptually looks like:

<table>
    <thead>
        <td>number</td>
        <td>times2</td>
        <td>square</td>
    </thead>
    <tr>
        <td>0</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>1</td>
        <td>2</td>
        <td>1</td>
    </tr>
    <tr>
        <td>2</td>
        <td>4</td>
        <td>4</td>
    </tr>
    <tr>
        <td>3</td>
        <td>6</td>
        <td>9</td>
    </tr>
    <tr>
        <td>4</td>
        <td>8</td>
        <td>16</td>
    </tr>
    <tr>
        <td>5</td>
        <td>10</td>
        <td>25</td>
    </tr>
    <tr>
        <td>6</td>
        <td>12</td>
        <td>36</td>
    </tr>
    <tr>
        <td>7</td>
        <td>14</td>
        <td>49</td>
    </tr>
    <tr>
        <td>8</td>
        <td>16</td>
        <td>64</td>
    </tr>
    <tr>
        <td>9</td>
        <td>18</td>
        <td>81</td>
    </tr>
</table>

Let's try this one more time. This time, instead of generating numbers, we'll convert some data we already have as a
list of `Child` objects into a record stream.

```java
Field<String> firstName = new Field<>("firstName");
Field<String> lastName = new Field<>("lastName");
Field<Integer> favoriteNumber = new Field<>("favoriteNumber");

RecordStream favoriteNumbers = RecordStream.aux(children.stream())
    .mapToRecord(into -> into
        .field(firstName, Child::getFirstName)
        .field(lastName, Child::getLastName)
        .field(favoriteNumber, Child::getFavoriteNumber)
    );
```

This data conceptually looks like:

<table>
    <thead>
        <td>firstName</td>
        <td>lastName</td>
        <td>favoriteNumber</td>
    </thead>
    <tr>
        <td>Amelia</td>
        <td>Rose</td>
        <td>7</td>
    </tr>
    <tr>
        <td>James</td>
        <td>Johnson</td>
        <td>7</td>
    </tr>
    <tr>
        <td>Maria</td>
        <td>Cabrero</td>
        <td>4</td>
    </tr>
    <tr>
        <td>Lisa</td>
        <td>Woods</td>
        <td>100</td>
    </tr>
    <tr>
        <td>Marc</td>
        <td>Vincent</td>
        <td>3</td>
    </tr>
    <tr>
        <td>Tyler</td>
        <td>Laine</td>
        <td>9</td>
    </tr>
    <tr>
        <td>Olivia</td>
        <td>Pineau</td>
        <td>2</td>
    </tr>
    <tr>
        <td>Sunder</td>
        <td>Suresh</td>
        <td>22</td>
    </tr>
    <tr>
        <td>Megan</td>
        <td>Alis</td>
        <td>4</td>
    </tr>
</table>

If we wanted, we could relate children's favorite numbers with more info about those numbers, by joining our two data
sets together:

```java
RecordStream joinedNumbers = favoriteNumbers
    .leftJoin(numbers,
              on -> on.match((left, right) -> Objects.equals(left.get(favoriteNumber), right.get(number))),
              select -> select
                  .leftAllFields()
                  .rightAllFieldsExcept(number)
    );
```

We left-join the `favoriteNumbers` to the `numbers`, providing a join condition that matches when the left-side record's
`favoriteNumber` is equal to the right-side record's `number`. For our outgoing records, we select all fields from the
left-side record and all fields from the right-side record (excluding `number`, since it will be redundant with
`favoriteNumber`). The resulting data conceptually looks like:

<table>
    <thead>
        <td>firstName</td>
        <td>lastName</td>
        <td>favoriteNumber</td>
        <td>times2</td>
        <td>square</td>
    </thead>
    <tr>
        <td>Amelia</td>
        <td>Rose</td>
        <td>7</td>
        <td>14</td>
        <td>49</td>
    </tr>
    <tr>
        <td>James</td>
        <td>Johnson</td>
        <td>7</td>
        <td>14</td>
        <td>49</td>
    </tr>
    <tr>
        <td>Maria</td>
        <td>Cabrero</td>
        <td>4</td>
        <td>8</td>
        <td>16</td>
    </tr>
    <tr>
        <td>Lisa</td>
        <td>Woods</td>
        <td>100</td>
        <td>null</td>
        <td>null</td>
    </tr>
    <tr>
        <td>Marc</td>
        <td>Vincent</td>
        <td>3</td>
        <td>6</td>
        <td>9</td>
    </tr>
    <tr>
        <td>Tyler</td>
        <td>Laine</td>
        <td>9</td>
        <td>18</td>
        <td>81</td>
    </tr>
    <tr>
        <td>Olivia</td>
        <td>Pineau</td>
        <td>2</td>
        <td>4</td>
        <td>4</td>
    </tr>
    <tr>
        <td>Sunder</td>
        <td>Suresh</td>
        <td>22</td>
        <td>null</td>
        <td>null</td>
    </tr>
    <tr>
        <td>Megan</td>
        <td>Alis</td>
        <td>4</td>
        <td>8</td>
        <td>16</td>
    </tr>
</table>

While this join yields the results we expect, it is not efficient for larger input data. The problem is that the
`match()` lambda is opaque to Vinyl, so there is not enough information for Vinyl to optimize the join. When the join is
evaluated, for each left-side record, we will loop over the whole right side searching for records that match - a nested
loop. We could provide more information by writing the join condition differently:

```java
RecordStream joinedNumbers = favoriteNumbers
    .leftJoin(numbers,
              on -> on.eq(on.left(favoriteNumber), on.right(number)),
              select -> select
                  .leftAllFields()
                  .rightAllFieldsExcept(number)
    );
```

Now, Vinyl knows we are doing an equality test between the left and right sides. When the join is evaluated, we will
first index the right side, grouping records by their `number` value. Then, for each left-side record, we will look up
its `favoriteNumber` value in the index, quickly finding all right-side records that match.

Since a `RecordStream` is a `Stream`, we can still use any of the usual stream operations:

```java
int sumOfMSquares = joinedNumbers
    .filter(record -> record.get(firstName).startsWith("M"))
    .mapToInt(record -> record.get(square))
    .sum();
```

Or, if we need to use the same records again, we may store them in a `RecordSet`:

```java
RecordSet data = joinedNumbers.toRecordSet();
```

Like a `RecordStream`, a `RecordSet` has a single header shared by all its records. This means we can easily get back to
a `RecordStream` from a `RecordSet`:

```java
RecordStream reStream = data.stream();
```

---

Here is our full example again, with streams inlined:

```java
Field<Integer> number = new Field<>("number");
Field<Integer> times2 = new Field<>("times2");
Field<Integer> square = new Field<>("square");
Field<String> firstName = new Field<>("firstName");
Field<String> lastName = new Field<>("lastName");
Field<Integer> favoriteNumber = new Field<>("favoriteNumber");

RecordSet data = RecordStream.aux(children.stream())
    .mapToRecord(into -> into
        .field(firstName, Child::getFirstName)
        .field(lastName, Child::getLastName)
        .field(favoriteNumber, Child::getFavoriteNumber)
    )
    .leftJoin(RecordStream.aux(IntStream.range(0, 10).boxed())
                  .mapToRecord(into -> into
                      .field(number, i -> i)
                      .field(times2, i -> i + i)
                      .field(square, i -> i * i)
                  ),
              on -> on.eq(on.left(favoriteNumber), on.right(number)),
              select -> select
                  .leftAllFields()
                  .rightAllFieldsExcept(number)
    )
    .toRecordSet();
```

---

This example gives a sense of how Vinyl models relational operations, but it does not explore the extent of what Vinyl
can do. For more information and examples, see the [package documentation](https://davery22.github.io/vinyl/javadoc/).

