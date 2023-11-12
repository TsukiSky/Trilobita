# Core

The `core` module is the backbone of the project, providing the primary functionalities and foundational components necessary for the operation and interaction of various components within the system.

Server module depends on this module.

## Table of Contents

- Features
- Usage
- Dependencies
- Contribution

## Features

1. **Common Utilities**:
   - `Util`: A utility class offering various general-purpose methods.
2. **Graph Components**:
   - `Graph`: Represents the core graph structure.
   - `Vertex`: Basic unit of the graph representing a node.
   - `Edge`: Represents connections between vertices.
   - `VertexGroup`: Encapsulates a group of vertices for bulk operations.
3. **Messaging System**:
   - `MessageAdmin` is a singleton class that offers functionalities related to Kafka topics. It allows you to:
     - Retrieve all existing topics.
     - Create new topics if they don't exist.
   - `MessageConsumer` consumes messages from a specified Kafka topic. It provides the ability to:
     - Start listening to a topic in a separate thread.
     - Stop the consumer thread.
     - Change the topic and restart the consumer.
     - Handle consumed messages using a custom `MessageHandler` interface.
   - `MessageProducer` enables the production of messages to a specified Kafka topic. It takes care of:
     - Creating the topic if it does not exist.
     - Sending messages to the desired topic.

## Usage

```java
// Example of using Vertex class
List<PageRankVertex> vertices = new ArrayList<>();
PageRankVertex vertex1 = new PageRankVertex(1);
vertex1.setStatus(Vertex.VertexStatus.ACTIVE);
vertices.add(vertex1);

PageRankVertex vertex2 = new PageRankVertex(2);
vertex2.setStatus(Vertex.VertexStatus.ACTIVE);
vertices.add(vertex2);

vertex1.addEdge(vertex2);

Graph graph = new Graph(vertices);

// Using the Messaging system
// Initialize MessageAdmin
private final MessageAdmin messageAdmin = MessageAdmin.getInstance();

//Produce a Message:
UUID key = UUID.randomUUID();
Mail mail = new Mail("Hello, Kafka!");
String targetTopic = "sample_topic";
MessageProducer.produce(key, mail, targetTopic);

//Consume Messages:
//Implement the MessageHandler interface:
MessageConsumer.MessageHandler handler = new MessageConsumer.MessageHandler() {
    @Override
    public void handleMessage(UUID key, Mail value, int partition, long offset) {
        System.out.println("Received message: " + value.getContent());
    }
};

MessageConsumer consumer = new MessageConsumer("sample_topic", 1, handler);
consumer.start();
```

## Dependencies

- Ensure the `core` module is included in your Maven dependencies if you intend to use its functionalities in other modules.

```xml
<dependency>
    <groupId>com.trilobita</groupId>
    <artifactId>core</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Contribution

Contributions to the `core` module are always welcome. Please adhere to the standard pull request process, and ensure that you write unit tests for any new features or fixes. Thoroughly document your changes.