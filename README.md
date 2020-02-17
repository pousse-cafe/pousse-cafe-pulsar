![Travis build status](https://travis-ci.org/pousse-cafe/pousse-cafe-pulsar.svg?branch=master)
![Maven status](https://maven-badges.herokuapp.com/maven-central/org.pousse-cafe-framework/pousse-cafe-pulsar/badge.svg)

# Pousse-Café Pulsar

This projects enables the use of [Apache Pulsar](https://pulsar.apache.org/) as messaging backend.
A JSON (Jackson-based) codec is being used for messages which are transmitted as text.

Create a `PulsarMessaging` instance and use it when
[running your model](http://www.pousse-cafe-framework.org/doc/reference-guide/#run-your-model).

If you are creating a Spring app, you might want to check 
[Pousse-Café Spring Pulsar](https://github.com/pousse-cafe/pousse-cafe-spring-pulsar) project, which registers Pulsar
messaging as a Spring Bean and retrieves configuration from external properties.

## Configure your Maven project

Add the following snippet to your POM:

    <dependency>
        <groupId>org.pousse-cafe-framework</groupId>
        <artifactId>pousse-cafe-pulsar</artifactId>
        <version>${poussecafe.pulsar.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.pulsar</groupId>
        <artifactId>pulsar-client</artifactId>
        <version>${pulsar.version}</version>
    </dependency>
