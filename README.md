# Vinyl: Relational Streams for Java

Vinyl is now in beta. Feedback is welcome!

Inspired by SQL, built for Java. Vinyl takes the expressive power of SQL queries and reimagines it on top of Java
Streams. By leaning into existing JDK abstractions, Vinyl achieves seamless integration with Java Streams, leveraging
their efficiency and parallelism while enhancing their capability. A carefully controlled API enables relational
operations to prioritize safety without sacrificing brevity.

For more information and examples, see the [package documentation](https://davery22.github.io/vinyl/javadoc/).

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
