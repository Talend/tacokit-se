## Usage of Common-Test module

This module aims to help to build unit test.


### Add depedency on pom.xml
```xml
    <dependency>
        <groupId>org.talend.components</groupId>
        <artifactId>common-test</artifactId>
        <version>${project.version}</version>
        <scope>test</scope>
    </dependency>
```

### Use of Generator

Objective of Generator class is to help to add tests on long list of Record.

1 : implements interface AssertionsBuilder<T>. 
This will help to build a fonction to check object T while building a new Record. This listening several step of record building (start, add field and end).

```java
public class MyObjectChecker implements AssertionsBuilder<MyObject> {
   ... 
}
```

<br/><br/>
2 : build a parameterized test

```java
    @ParameterizedTest
    @MethodSource("testSource")
    void testMyObject(DataSet<MyObject> ds) {
        final MyObject result = xxx(ds.getRecord(), ...);
        ds.check(result);
    }

    private static Iterator<DatasetGenerator.DataSet<MyObject>> testMyObject() {
        final AssertionsBuilder<MyObject> valueBuilder = new MyObjectChecker();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        DatasetGenerator<MyObject> generator = new DatasetGenerator<>(factory, valueBuilder);  // you can also use your own RecordGenerator (see constructor)
        return generator.generate(40); // 40 define collecrtion size.
    }
```

for an example, you can see unit test on stream-json module for example.


<br/><br/>
**NB** : This module is not design to be limited to data checker generator, feel free to add package & classes that can help unit test for several modules. 
