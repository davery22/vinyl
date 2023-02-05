# Cheat Sheet

Comparisons between Vinyl and SQL.

For brevity, the SQL examples assume that any referenced tables already exist, with an appropriate schema. The Vinyl 
examples assume that record-sets already exist (corresponding to the tables referenced in the SQL examples), and that
any referenced fields also already exist.

---

<a id="table-of-contents"></a>
## Table of Contents

1. [Expression](#expression)
2. [Where](#where)
3. [Order By](#order-by)
4. [Limit & Offset](#limit-offset)
5. [Aggregate](#aggregate)
6. [Distinct](#distinct)
7. [Union](#union)
8. [Intersect & Except](#intersect-except)
9. [Window](#window)
10. [Join](#join)
11. [Semi Join](#semi-join)
12. [Lateral Join (Apply)](#lateral-join)
13. [Pivot](#pivot)
14. [Unpivot](#unpivot)
15. [Recursive Query](#recursive-query)
16. [Divide](#divide)

---

<a id="expression"></a>
## Expression [↩](#table-of-contents)︎

SQL
``` sql
SELECT
    number,
    ABS(number) AS abs,
    (number * (number + 1)) / 2 AS sum
FROM numbers
```

Java - Vinyl
``` java
RecordStream result = numbers.stream()
    .select(select -> select
        .field(number)
        .field(abs, record -> Math.abs(record.get(number)))
        .field(sum, record -> (record.get(number) * (record.get(number) + 1)) / 2)
    );
```

---

<a id="where"></a>
## Where [↩](#table-of-contents)︎

SQL
``` sql
SELECT *
FROM employees
WHERE "Jonas" = first_name
```

Java - Vinyl
``` java
RecordStream result = employees.stream()
    .filter(emp -> "Jonas".equals(emp.get(firstName)));
```

---

<a id="order-by"></a>
## Order By [↩](#table-of-contents)︎

SQL
``` sql
SELECT *
FROM competitors
ORDER BY rank
```

Java - Vinyl
``` java
RecordStream result = competitors.stream()
    .sorted(Comparator.comparing(rank::get));
```

---

SQL
``` sql
SELECT *
FROM competitors
ORDER BY rank ASC NULLS LAST
```

Java - Vinyl
``` java
RecordStream result = competitors.stream()
    .sorted(Comparator.comparing(rank::get, Comparator.nullsLast(Comparator.naturalOrder())));
```

---

SQL
``` sql
SELECT *
FROM competitors
ORDER BY rank ASC NULLS LAST, first_name, last_name DESC
```

Java - Vinyl
``` java
RecordStream result = competitors.stream()
    .sorted(Comparator.comparing(rank::get, Comparator.nullsLast(Comparator.naturalOrder()))
                      .thenComparing(firstName::get)
                      .thenComparing(lastName::get, Comparator.reverseOrder())
    );
```

Note that Java Comparators are not null-safe by default. If a sort field may contain nulls, it should qualify either
`nullsFirst()` or `nullsLast()`.

---

<a id="limit-offset"></a>
## Limit & Offset [↩](#table-of-contents)︎

SQL
``` sql
SELECT *
FROM submissions
ORDER BY score
LIMIT 10 OFFSET 5
```

Java - Vinyl
``` java
RecordStream result = submissions.stream()
    .sorted(Comparator.comparing(score::get))
    .limit(10)
    .skip(5);
```

Note that this example sorts all the records, before skipping up to 5 and yielding up to 10 (in order). If multiple
queries are needed (eg for paging), it may be prudent to store the sorted records in a `RecordSet` first, to avoid
repeated sorting.

---

<a id="aggregate"></a>
## Aggregate [↩](#table-of-contents)︎

SQL
``` sql
SELECT
    COUNT(1) AS count,
    SUM(quantity) AS sum,
    AVG(unit_price) AS avg
FROM orders
```

Java - Vinyl
``` java
RecordStream result = orders.stream()
    .aggregate(aggregate -> aggregate
        .aggField(count, Collectors.counting())
        .aggField(sum, Collectors.summingLong(quantity::get))
        .aggField(avg, Collectors.averagingLong(unitPrice::get))
    );
```

---

SQL
``` sql
SELECT
    product
    COUNT(1) AS count,
    SUM(quantity) AS sum,
    AVG(unit_price) AS avg
FROM orders
WHERE 'Evil Corp' <> vendor
GROUP BY vendor, product
HAVING COUNT(1) > 3
```

Java - Vinyl
``` java
RecordStream result = orders.stream()
    .filter(record -> !"Evil Corp".equals(record.get(vendor)))
    .aggregate(aggregate -> aggregate
        .key(vendor)
        .keyField(product)
        .aggField(count, Collectors.counting())
        .aggField(sum, Collectors.summingLong(quantity::get))
        .aggField(avg, Collectors.averagingLong(unitPrice::get))
    )
    .filter(record -> record.get(count) > 3);
```

---

<a id="distinct"></a>
## Distinct [↩](#table-of-contents)︎

SQL allows `DISTINCT` to be used in a `SELECT` clause, or in an aggregate function. These different usages are
translated to Vinyl differently.

SQL
``` sql
SELECT DISTINCT city, state, zip
FROM area_ratings
```

Java - Vinyl
``` java
RecordStream result = areaRatings.stream()
    .aggregate(aggregate -> aggregate.keyFields(city, state, zip));
```

---

SQL
``` sql
SELECT
    COUNT(DISTINCT city) count_cities,
    AVG(DISTINCT rating) avg_distinct_rating,
    SUM(DISTINCT rating) sum_distinct_ratings
FROM area_ratings
```

Java - Vinyl
``` java
RecordStream result = areaRatings.stream()
    .aggregate(aggregate -> aggregate
        .aggField(countCities, Collectors.collectingAndThen(Collectors.mapping(city::get, Collectors.toSet()), Set::size))
        .aggFields(Collectors.mapping(rating::get, Collectors.toSet()), fields -> fields
            .aggField(avgDistinctRating,  ratings -> ratings.stream().mapToLong(rating -> rating).average().orElse(0))
            .aggField(sumDistinctRatings, ratings -> ratings.stream().mapToLong(rating -> rating).sum())
        )
    );
```

Note that the above example will create exactly one Set of ratings, used by both the `avgDistinctRating` and the
`sumDistinctRatings` fields.

---

<a id="union"></a>
## Union [↩](#table-of-contents)︎

Vinyl offers `RecordStream.concat()` for unioning two record-streams that have matching headers. This method behaves
similarly to `Stream.concat()`. In particular, `Stream.concat()` documents that

> Accessing an element of a deeply concatenated stream can result in deep call chains, or even `StackOverflowError`.

and suggests an alternative for repeated concatenation: `flatMap()`. If the `flatMap()` alternative is used in Vinyl,
we must map the resulting `RecordStream.Aux` back to a `RecordStream`.

Either approach leaves duplicates intact (analogous to SQL's `UNION ALL`). If this is not desired, duplicates can be
explicitly removed with a subsequent `.distinct()` operation.

SQL
``` sql
SELECT product_id
FROM products
UNION ALL
SELECT product_id
FROM work_orders
```

Java - Vinyl
``` java
// Using concat():
RecordStream result =
    RecordStream.concat(
        products.stream()
            .select(select -> select.field(productId)),
        workOrders.stream()
            .select(select -> select.field(productId))
    );

// Using flatMap():
RecordStream result =
    RecordStream.aux(
        Stream.of(
            products.stream()
                .select(select -> select.field(productId)),
            workOrders.stream()
                .select(select -> select.field(productId))
        )
    )
    .flatMap(s -> s)
    .mapToRecord(into -> into.field(productId));
```

---

<a id="intersect-except"></a>
## Intersect & Except [↩](#table-of-contents)︎

Vinyl currently does not provide syntax for `intersect` and `except` operations. Instead, these operations can be
achieved by either:

1. Collecting the right-side into a Set, and filtering the left-side using the Set
2. Using a semi-join to filter the left-side based on whether there are any matching right-side records

Either approach leaves duplicates intact (analogous to SQL's `INTERSECT ALL` and `EXCEPT ALL`). If this is not desired,
duplicates can be explicitly removed with a subsequent `.distinct()` operation.

SQL
``` sql
SELECT product_id
FROM products
INTERSECT ALL
SELECT product_id
FROM work_orders
```

Java - Vinyl
``` java
// Using a filter() with a Set:
Set<Record> productIds = workOrders.stream()
    .select(select -> select.field(productId))
    .collect(Collectors.toSet());

RecordStream result = products.stream()
    .select(select -> select.field(productId))
    .filter(record -> productIds.contains(record));
    
// Using a semi-join:
RecordStream result = products.stream()
    .select(select -> select.field(productId))
    .leftAnyMatch(workOrders.stream(),
                  on -> on.eq(on.left(productId), on.right(productId))
    );
```

---

SQL
``` sql
SELECT product_id
FROM products
EXCEPT ALL
SELECT product_id
FROM work_orders
```

Java - Vinyl
``` java
// Using a filter() with a Set:
Set<Record> productIds = workOrders.stream()
    .select(select -> select.field(productId))
    .collect(Collectors.toSet());

RecordStream result = products.stream()
    .select(select -> select.field(productId))
    .filter(record -> !productIds.contains(record));
    
// Using a (anti) semi-join:
RecordStream result = products.stream()
    .select(select -> select.field(productId))
    .leftNoneMatch(workOrders.stream(),
                   on -> on.eq(on.left(productId), on.right(productId))
    );
```

---

<a id="window"></a>
## Window [↩](#table-of-contents)︎

todo

---

<a id="join"></a>
## Join [↩](#table-of-contents)︎

todo

---

<a id="semi-join"></a>
## Semi Join [↩](#table-of-contents)︎

A relational semi join filters records from a dataset, based on the presence of matching records in another dataset.

Most SQL dialects do not provide syntax for semi joins. In SQL, semi joins can be achieved using `WHERE [NOT] EXISTS`,
`ANY`, and `ALL`.

This could be directly translated to Vinyl using a `filter()` with an inner `anyMatch()`/`allMatch()`/`noneMatch()`.
However, due to the lack of persistent indexes in Vinyl, the inner test would be inefficient. When the inner dataset is
"uncorrelated" (ie does not depend on the outer dataset), Vinyl offers more efficient alternatives, that create a
temporary index on the inner dataset.

``` sql
```

``` java
```

---

<a id="lateral-join"></a>
## Lateral Join (Apply) [↩](#table-of-contents)︎

Vinyl does not provide syntax for SQL `LATERAL` joins (sometimes spelled `CROSS APPLY` or `OUTER APPLY`). Instead, these
operations can be achieved with a `flatMap()`, followed by mapping the resulting `RecordStream.Aux` back to a
`RecordStream`.

SQL
``` sql

```

Java - Vinyl
``` java

```

---

SQL
``` sql

```

Java - Vinyl
``` java

```

---

<a id="pivot"></a>
## Pivot [↩](#table-of-contents)︎

Vinyl does not provide syntax for a `pivot` operation. Instead, this operation can be achieved with the `route()`
feature of Vinyl's aggregates.

Note: This example uses [Databricks SQL `PIVOT` syntax](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-pivot.html),
since it is especially expressive yet concise. In many SQL dialects, pivot-like behavior can be achieved using a
`GROUP BY` with an aggregate function per pivoted column, and `CASE` to filter values within the aggregate functions.

SQL
``` sql
SELECT location, year, q1, q2, q3, q4
FROM quarterly_sales
PIVOT (
    SUM(sales) AS sales
    FOR quarter
    IN (
        'Jan-Mar' AS q1,
        'Apr-Jun' AS q2,
        'Jul-Sep' AS q3,
        'Oct-Dec' AS q4
    )
)
```

Java - Vinyl
``` java
// Associate value(s) with field(s). In this case, they are 1:1.
Map<String, Field<Long>> assocs =
    Map.of(
        "Jan-Mar", q1,
        "Apr-Jun", q2,
        "Jul-Sep", q3,
        "Oct-Dec", q4
    );

// Route values to fields that aggregate over those values.
RecordStream result = quarterlySales.stream()
    .aggregate(aggregate -> aggregate
        .keyFields(location, year)
        .<Long>route(
            (record, sink) -> sink.accept(assocs.get(record.get(quarter)), record.get(sales)),
            route -> assocs.forEach((quarterValue, quarterSales) ->
                route.field(quarterSales, Collectors.summingLong(salesValue -> salesValue))
            )
        )
    );
```

---

<a id="unpivot"></a>
## Unpivot [↩](#table-of-contents)︎

Vinyl does not provide syntax for an `unpivot` operation. Instead, this operation can be achieved with a `flatMap()`,
followed by mapping the resulting `RecordStream.Aux` back to a `RecordStream`.

Note: This example uses [Databricks SQL `UNPIVOT` syntax](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-unpivot.html),
since it is especially expressive yet concise. In many SQL dialects, unpivot-like behavior can be achieved using a
`CROSS JOIN LATERAL` or `CROSS APPLY` to yield a row per pivoted column. (This is also essentially how Vinyl models the
operation.)

SQL
``` sql
SELECT location, year, quarter, sales
FROM sales_by_quarter
UNPIVOT (
    sales
    FOR quarter
    IN (
        q1 AS 'Jan-Mar',
        q2 AS 'Apr-Jun',
        q3 AS 'Jul-Sep',
        q4 AS 'Oct-Dec'
    )
)
```

Java - Vinyl
``` java
// Associate field(s) with value(s). In this case, they are 1:1.
// Here we just roll our own type to model the association.
record Assoc(Field<Long> quarterSales, String quarter) {}
List<Assoc> assocs =
    List.of(
        new Assoc(q1, "Jan-Mar"),
        new Assoc(q2, "Apr-Jun"),
        new Assoc(q3, "Jul-Sep"),
        new Assoc(q4, "Oct-Dec")
    );
    
// Cross each record with each association, and map the resulting items back to records.
RecordStream result = salesByQuarter.stream()
    .flatMap(record -> assocs.stream()
        .map(assoc -> new Object(){ Record record = record; Assoc assoc = assoc; })
    )
    .mapToRecord(into -> into
        .field(location, o -> o.record.get(location))
        .field(year, o -> o.record.get(year))
        .field(quarter, o -> o.assoc.quarter)
        .field(sales, o -> o.record.get(o.assoc.quarterSales))
    );
```

---

<a id="recursive-query"></a>
## Recursive Query [↩](#table-of-contents)︎

SQL
``` sql
WITH managers AS (
    SELECT *
    FROM staff
    WHERE manager_id IS NULL
    UNION ALL
    SELECT s.*
    FROM staff s
    JOIN managers m
        ON s.manager_id = m.id
)
SELECT *
FROM managers
```

Java - Vinyl
``` java
RecordStream result =
    RecordStream.aux(
        Stream.iterate(
            staff.stream()
                .filter(s -> s.get(managerId) == null)
                .toRecordSet(),
            managers -> !managers.isEmpty(),
            managers -> staff.stream()
                .join(managers.stream(),
                      on -> on.eq(on.left(managerId), on.right(id)),
                      JoinAPI.Select::allLeftFields
                )
                .toRecordSet()
        )
    )
    .flatMap(RecordSet::stream)
    .mapToRecord(staff.header()::selectAllFields);
```

---

<a id="divide"></a>
## Divide [↩](#table-of-contents)︎

Relational division is a rare operation, that acts like the inverse of a cross join. For a description and example of
relational division, see [this jOOQ blog post](https://blog.jooq.org/advanced-sql-relational-division-in-jooq/).

Most SQL dialects do not provide syntax for relational division. In SQL, relational division can be modeled as a
"double-negative", using nested `WHERE NOT EXISTS` clauses:

SQL
``` sql
SELECT DISTINCT c1.student
FROM completed c1
WHERE NOT EXISTS (
    SELECT 1
    FROM db_projects db
    WHERE NOT EXISTS (
        SELECT 1
        FROM completed c2
        WHERE c2.student = c1.student
            AND c2.task = db.task
    )
)
```

Vinyl also does not provide syntax for relational division. The above SQL could be directly translated:

``` java
RecordStream result = completed.stream()
    .filter(c1 -> dbProjects.stream()
        .noneMatch(db -> completed.stream()
            .noneMatch(c2 -> c2.get(student).equals(c1.get(student))
                          && c2.get(task).equals(db.get(task))
            )
        )
    )
    .aggregate(aggregate -> aggregate.keyField(student));
```

But this would be a very inefficient approach. Instead, leaning into its support for arbitrary field types, Vinyl can
avoid the double-negative:

``` java
Set<String> dbTasks = dbProjects.stream()
    .map(task::get)
    .collect(Collectors.toSet());

RecordStream result = completed.stream()
    .aggregate(aggregate -> aggregate
        .keyField(student)
        .aggField(tasks, Collectors.mapping(task::get, Collectors.toSet()))
    )
    .filter(record -> record.get(tasks).containsAll(dbTasks))
    .select(select -> select.field(student));
```
