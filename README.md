# Azure Service Bus WSO2 MI Inbound Endpoint

The Azure Service Bus (ASB) inbound endpoint acts as an event-based message consumer for
[Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/). It connects to a
Service Bus namespace using a connection string and consumes messages from a **queue** or a **topic
subscription**, injecting each message into a mediation sequence.

It is built on top of the Azure `ServiceBusProcessorClient`, so it supports concurrent consumption,
automatic lock renewal, configurable retry behavior for transient failures, and message settlement
(`complete` / `abandon` / `defer` / `deadLetter`) in `PEEK_LOCK` mode.

## Compatibility

| Inbound Endpoint version | Supported Azure SDK version | Supported WSO2 MI version |
| ------------------------ | --------------------------------- | ---------------------------- |
| 1.0.0                    | azure-messaging-servicebus 7.17.7 | MI 4.4.0, MI 4.5.0, MI 4.6.0 |

## Getting started

To get started with the inbound endpoint, see [Configuring the Azure Service Bus Inbound Endpoint](docs/config.md).
Once configured, the endpoint consumes messages from the target queue or topic subscription and injects
them into the configured sequence.

> **Note:** Session-enabled queues and subscriptions are not supported. See the
> [configuration guide](docs/config.md) for details.

## Building from the source

Follow the steps below to build the Azure Service Bus Inbound Endpoint from the source code.

1. Clone or download the source from [GitHub](https://github.com/wso2-extensions/mi-inbound-asb).
2. Run the following Maven command from the `mi-inbound-asb` directory: `mvn clean install`.
3. The JAR file for the inbound endpoint is created in the `mi-inbound-asb/target` directory.

## How you can contribute

As an open source project, WSO2 extensions welcome contributions from the community.
Check the [issue tracker](https://github.com/wso2-extensions/mi-inbound-asb/issues) for open issues that
interest you. We look forward to receiving your contributions.
