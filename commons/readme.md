# Commons Module

The `commons` module provides basic utilities and foundational components essential for the smooth functioning of the project. It encapsulates common functionalities that can be reused across different modules.

## Table of Contents

- Features
- Usage
- Dependencies
- Contribution

## Features

1. **Common Classes**:
   - `Computable`: An interface defining computable entities. (add, minus, multiply, divide)
   - `Mail`: Represents a mail entity with related attributes and behaviors.
   - `Message`: Represents a message entity.
2. **Serialization and Deserialization**:
   - `MailSerializer`: Provides functionalities to serialize mail objects during communication in `kafka.properties`.
   - `MailDeserializer`: Offers functionalities to deserialize mail objects during communication in `kafka.properties`.
3. **Custom Exception**:
   - `TrilobitaException`: A custom exception class for handling project-specific exceptions.

## Usage

#### Mail

```java
// Example of using Mail class
Mail newMail = new Mail(toVertexId, null, Mail.MailType.NORMAL);
```

#### MailSerializer

```java
// Serialization of Mail object in `kafka.properties`
// Deserialization of Mail object in `kafka.properties`
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=com.trilobita.serializer.MailSerializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=com.trilobita.deserializer.MailDeserializer
```

#### MailDeSerializer

```java
// Example of using TrilobitaException class
public abstract void start() throws TrilobitaException, InterruptedException, ExecutionException;
```

## Dependencies

- Ensure you have imported the `commons` module in your Maven dependencies if you wish to use it in other modules.

```xml
<dependency>
    <groupId>com.trilobita</groupId>
    <artifactId>commons</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## Contribution

If you'd like to contribute to the `commons` module, please follow the standard pull request process. Make sure to write unit tests for any new feature or fix and document your changes thoroughly.

