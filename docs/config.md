# Configuring the Azure Service Bus Inbound Endpoint

The Azure Service Bus (ASB) inbound endpoint acts as an event-based message consumer. It connects to an
Azure Service Bus namespace using a connection string and consumes messages from either a **queue** or a
**topic subscription**, injecting each message into a mediation sequence.

The endpoint is built on top of the Azure `ServiceBusProcessorClient`, so it supports concurrent
consumption, automatic lock renewal, and configurable retry behavior for transient failures.

## Prerequisites

- An Azure Service Bus namespace with at least one queue, or a topic with a subscription.
- A connection string with `Listen` (or higher) permission for the target entity. You can obtain this
  from the Azure portal under **Shared access policies**.

## Receive modes and message settlement

The endpoint supports two receive modes, controlled by the `receiveMode` parameter:

- **`PEEK_LOCK`** (default) — the message is locked on the broker while your sequence processes it. The
  message is **not** removed until the mediation flow explicitly settles it. Use the
  **Message Settlement (Only For Event Integration)** operations in the Azure Service Bus connector to
  record one of the following decisions:
  - `complete` — remove the message (successful processing).
  - `abandon` — release the lock so the message is redelivered.
  - `defer` — defer the message for later retrieval by sequence number.
  - `deadLetter` — move the message to the dead-letter queue (optionally with a reason and description).

  If the sequence does not record a decision before `messageProcessingTimeoutMs` elapses, the lock is
  allowed to expire and the message is redelivered by Azure (at-least-once delivery). Azure enforces the
  entity's max delivery count, after which repeatedly un-settled messages are dead-lettered automatically.

- **`RECEIVE_AND_DELETE`** — the message is deleted from the broker as soon as it is received. No
  settlement is performed and there is no redelivery (at-most-once delivery). The settlement, lock, and
  processing-timeout parameters are ignored in this mode.

> **Limitation: session-enabled entities are not supported.** This inbound endpoint uses a non-session
> Service Bus processor client. Pointing it at a **session-enabled** queue or topic subscription
> (created with *Enable sessions* / `requiresSession = true`) will fail at runtime, because the Azure
> SDK requires a dedicated session receiver for such entities. Use only non-session queues and
> subscriptions.

## Sample configuration

### Consuming from a queue (PEEK_LOCK)

```xml
<inboundEndpoint xmlns="http://ws.apache.org/ns/synapse"
                 name="asbQueueListener"
                 sequence="request"
                 onError="fault"
                 class="org.wso2.carbon.inbound.asb.ASBEventConsumer"
                 suspend="false">
   <parameters>
      <parameter name="inbound.behavior">eventBased</parameter>
      <parameter name="coordination">true</parameter>
      <parameter name="connectionString">Endpoint=sb://&lt;namespace&gt;.servicebus.windows.net/;SharedAccessKeyName=&lt;keyName&gt;;SharedAccessKey=&lt;key&gt;</parameter>
      <parameter name="entityType">queue</parameter>
      <parameter name="queueName">orders</parameter>
      <parameter name="receiveMode">PEEK_LOCK</parameter>
      <parameter name="maxConcurrentMessages">1</parameter>
      <parameter name="maxLockDurationMs">300000</parameter>
      <parameter name="messageProcessingTimeoutMs">240000</parameter>
      <parameter name="contentType">application/json</parameter>
   </parameters>
</inboundEndpoint>
```

### Consuming from a topic subscription

```xml
<inboundEndpoint xmlns="http://ws.apache.org/ns/synapse"
                 name="asbTopicListener"
                 sequence="request"
                 onError="fault"
                 class="org.wso2.carbon.inbound.asb.ASBEventConsumer"
                 suspend="false">
   <parameters>
      <parameter name="inbound.behavior">eventBased</parameter>
      <parameter name="coordination">true</parameter>
      <parameter name="connectionString">Endpoint=sb://&lt;namespace&gt;.servicebus.windows.net/;SharedAccessKeyName=&lt;keyName&gt;;SharedAccessKey=&lt;key&gt;</parameter>
      <parameter name="entityType">topic</parameter>
      <parameter name="topicName">events</parameter>
      <parameter name="subscriptionName">audit</parameter>
      <parameter name="receiveMode">PEEK_LOCK</parameter>
      <parameter name="contentType">application/json</parameter>
   </parameters>
</inboundEndpoint>
```

## Inbound endpoint parameters

### Connection

| Parameter | Description | Required | Possible values |
| --------- | ----------- | -------- | --------------- |
| `connectionString` | Azure Service Bus connection string used to authenticate to the namespace. | Yes | String |
| `entityType` | Type of Service Bus entity to consume from. | Yes | `queue`, `topic` |
| `queueName` | Name of the queue to consume from. | Required when `entityType` is `queue` | String |
| `topicName` | Name of the topic to consume from. | Required when `entityType` is `topic` | String |
| `subscriptionName` | Name of the topic subscription to consume from. | Required when `entityType` is `topic` | String |
| `contentType` | Content type used to build the message payload. Takes priority over the message's own content type. If neither is set, the SOAP builder is used. | No | `application/json`, `application/xml`, `text/plain`, ... |
| `coordination` | In a clustered setup, runs the inbound only on a single worker node. | No | `true` (default), `false` |
| `inboundVariableName` | Name of the variable that holds the structured message data (attributes and headers). | No | String (default `asb_inbound`) |

### Consumer

| Parameter | Description | Required | Possible values |
| --------- | ----------- | -------- | --------------- |
| `receiveMode` | Message receive mode. `PEEK_LOCK` allows settling messages after processing; `RECEIVE_AND_DELETE` removes messages immediately. | No | `PEEK_LOCK` (default), `RECEIVE_AND_DELETE` |
| `maxConcurrentMessages` | Maximum number of messages processed concurrently. | No | Integer (default `1`) |
| `prefetchCount` | Number of messages fetched from the broker and buffered locally. Higher values improve throughput but use more memory. `0` disables prefetching. | No | Integer (default `0`) |
| `maxLockDurationMs` | Maximum duration (ms) to keep the message lock alive during processing. The lock is auto-renewed in the background until this duration is reached. PEEK_LOCK only. | No | Long (default `300000`) |
| `messageProcessingTimeoutMs` | Maximum time (ms) to wait for a message to be fully processed (settled) before timing out. Should be lower than `maxLockDurationMs`. On timeout the lock expires and the message is redelivered. PEEK_LOCK only. | No | Long (default `240000`) |

### Resilience

These map to the Azure AMQP retry options (exponential backoff) and apply to transient connection or
network failures.

| Parameter | Description | Required | Possible values |
| --------- | ----------- | -------- | --------------- |
| `maxRetries` | Maximum number of retry attempts for transient failures. | No | Integer (default `3`) |
| `retryDelayMs` | Initial delay (ms) between retries. Increases exponentially up to `retryMaxDelayMs`. | No | Long (default `1000`) |
| `retryMaxDelayMs` | Maximum delay (ms) between retries. | No | Long (default `30000`) |
| `tryTimeoutMs` | Maximum duration (ms) to wait for a single operation (e.g. receive, settle) before timing out. | No | Long (default `60000`) |

## Accessing message metadata in the sequence

Each injected message exposes Service Bus metadata to the mediation flow in three ways:

### Structured variable

A variable (default name `asb_inbound`, configurable via `inboundVariableName`) is set on the message
context containing two child maps:

**`attributes`** — ASB-specific message attributes:

| Key | Description |
| --- | ----------- |
| `timeToLive` | Message time-to-live. |
| `deliveryCount` | Number of times the message has been delivered. |
| `enqueuedTime` | Time the message was enqueued. |
| `sequenceNumber` | Broker-assigned sequence number. |
| `partitionKey` | Partition key (set only if present). |
| `to` | The `To` address (set only if present). |
| `deadLetterSource` | Original entity if the message came from a dead-letter queue (set only if present). |

**`headers`** — messaging headers and application properties:

| Key | Description |
| --- | ----------- |
| `messageId` | Message id. |
| `correlationId` | Correlation id. |
| `contentType` | Message content type. |
| `subject` | Message subject/label. |
| `replyTo` | Reply-to address. |
| *(custom keys)* | Any application properties set by the sender. |

### Synapse message context properties

The following are set as **Synapse message context properties** (access with `get-property('<name>')`):

| Property | Description |
| -------- | ----------- |
| `asb.timeToLive` | Message time-to-live. |
| `asb.deliveryCount` | Number of times the message has been delivered. |
| `asb.enqueuedTime` | Time the message was enqueued. |
| `asb.sequenceNumber` | Broker-assigned sequence number. |
| `asb.partitionKey` | Partition key (set only if present). |
| `asb.to` | The `To` address (set only if present). |
| `asb.deadLetterSource` | Original entity if the message came from a dead-letter queue (set only if present). |

### Transport headers

The following are set as **transport headers** (access with `get-property('transport', '<name>')`), along
with every custom application property carried on the message:

| Header | Description |
| ------ | ----------- |
| `asb.messageId` | Message id. |
| `asb.correlationId` | Correlation id. |
| `asb.contentType` | Message content type. |
| `asb.subject` | Message subject/label. |
| `asb.replyTo` | Reply-to address. |

## Configuring a sample sequence

```xml
<sequence xmlns="http://ws.apache.org/ns/synapse" name="request" onError="fault">
   <log level="custom">
      <property name="messageId" expression="get-property('transport', 'asb.messageId')"/>
      <property name="deliveryCount" expression="get-property('asb.deliveryCount')"/>
   </log>
   <log level="full"/>
   <!-- In PEEK_LOCK mode, record a settlement decision using the Azure Service Bus
        connector's settlement operation, for example: complete the message. -->
  <asb.consumer_complete />
</sequence>
```

A sample fault sequence:

```xml
<sequence xmlns="http://ws.apache.org/ns/synapse" name="fault">
   <log level="full">
      <property name="MESSAGE" value="Executing default 'fault' sequence"/>
      <property name="ERROR_CODE" expression="get-property('ERROR_CODE')"/>
      <property name="ERROR_MESSAGE" expression="get-property('ERROR_MESSAGE')"/>
   </log>
  <asb.consumer_deadLetter >
    <responseVariable >asb_consumer_deadLetter_1</responseVariable>
    <overwriteBody >false</overwriteBody>
    <deadLetterReason >DEADLETTERED_BY_RECEIVER</deadLetterReason>
    <deadLetterErrorDescription >Fault in processing</deadLetterErrorDescription>
  </asb.consumer_deadLetter>
   <drop/>
</sequence>
```
