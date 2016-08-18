# Apache Bahir

Apache Bahir provides extensions to distributed analytics platforms such as Apache Spark and Apache Flink.

<http://bahir.apache.org/>


## Building Bahir

Bahir is built using [Apache Maven](http://maven.apache.org/).
To build Bahir and its example programs, run:

    mvn -DskipTests clean install

## Running tests

Testing first requires [building Bahir](#building-bahir). Once Bahir is built, tests
can be run using:

    mvn test
