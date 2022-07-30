<!--
{% comment %}
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Apache Bahir (Flink)

Apache Bahir provides extensions to distributed analytics platforms such as Apache Spark™ and Apache Flink®.

<http://bahir.apache.org/>


This repository is for Apache Flink extensions.

## Contributing a Flink Connector

The Bahir community is very open to new connector contributions for Apache Flink.

We ask contributors to first open a [JIRA issue](http://issues.apache.org/jira/browse/BAHIR) describing the planned changes. Please make sure to put "Flink Streaming Connector" in the "Component/s" field.

Once the community has agreed that the planned changes are suitable, you can open a pull request at the "bahir-flink" repository.
Please follow the same directory structure as the existing code.

The community will review your changes, giving suggestions how to improve the code until we can merge it to the main repository.



## Building Bahir

Bahir is built using [Apache Maven](http://maven.apache.org/)™.
To build Bahir and its example programs, run:

    mvn -DskipTests clean install

## Running tests

Testing first requires [building Bahir](#building-bahir). Once Bahir is built, tests
can be run using:

    mvn test
