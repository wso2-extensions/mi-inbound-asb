/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.inbound.asb;

import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ASBEventConsumer extends GenericEventBasedConsumer {

    private static final Log LOG = LogFactory.getLog(ASBEventConsumer.class);

    private ServiceBusProcessorClient processorClient;
    private ASBMessageInjector messageInjector;
    private volatile boolean isDestroyed;
    private final AtomicInteger inFlightMessages = new AtomicInteger(0);

    private static final int DEFAULT_PREFETCH_COUNT = 0;
    private static final int DEFAULT_MAX_CONCURRENT_CONSUMERS = 1;
    private static final int DEFAULT_MAX_CONCURRENT_SESSIONS = 1;
    private static final int DEFAULT_MAX_CONCURRENT_CONSUMERS_PER_SESSION = 1;
    private static final long DEFAULT_SESSION_IDLE_TIMEOUT_MS = 60000;
    private static final long DEFAULT_MAX_LOCK_DURATION_MS = 300000;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_DELAY_MS = 1000;
    private static final long DEFAULT_RETRY_MAX_DELAY_MS = 30000;
    private static final long DEFAULT_TRY_TIMEOUT_MS = 60000;

    public ASBEventConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment,
                            String injectingSeq, String onErrorSeq,
                            boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, injectingSeq, onErrorSeq, coordination, sequential);
        LOG.info("Initializing ASB Consumer: " + name);
    }

    public void listen() {
        if (processorClient != null && !isDestroyed) {
            LOG.info("ASB Consumer '" + name + "' already running. Skipping start.");
            return;
        }
        LOG.info("Starting ASB Consumer: " + name);
        try {
            validateMandatoryParameters();
            this.messageInjector = createMessageInjector();
            this.processorClient = buildProcessorClient();
            processorClient.start();
            LOG.info("ASB Consumer '" + name + "' started successfully.");
        } catch (Exception e) {
            LOG.error("Failed to start ASB Consumer: " + name, e);
            throw new SynapseException("Failed to start ASB Consumer: " + name, e);
        }
    }

    private void processMessage(ServiceBusReceivedMessageContext context) {
        inFlightMessages.incrementAndGet();
        try {
            if (isDestroyed) {
                if (ServiceBusReceiveMode.PEEK_LOCK.equals(getReceiveMode())) {
                    LOG.warn("Consumer '" + name + "' is shutting down. Abandoning message. MessageId: "
                            + context.getMessage().getMessageId());
                    context.abandon();
                }
                return;
            }

            ServiceBusReceivedMessage message = context.getMessage();
            ServiceBusReceiveMode receiveMode = getReceiveMode();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing ASB message. MessageId: " + message.getMessageId());
            }
            MessageContext msgCtx = messageInjector.inject(context);

            if (ServiceBusReceiveMode.PEEK_LOCK.equals(receiveMode)) {
                settleMessage(context, msgCtx, message);
            }
        } finally {
            inFlightMessages.decrementAndGet();
        }
    }

    @SuppressWarnings("unchecked")
    private void settleMessage(ServiceBusReceivedMessageContext context, MessageContext msgCtx, ServiceBusReceivedMessage message) {
        Map<String, String> decision = null;
        Object holder = msgCtx.getProperty(ASBConstants.ASB_INBOUND_SETTLEMENT_DECISION);
        if (holder instanceof AtomicReference) {
            Object value = ((AtomicReference<?>) holder).get();
            if (value instanceof Map) {
                decision = (Map<String, String>) value;
            }
        }
        String action = (decision != null) ? decision.get(ASBConstants.DECISION_KEY_ACTION) : null;
        if (action != null) {
            settleMessage(context, decision, action);
        } else {
            LOG.warn("No settlement decision recorded for MessageId: " + message.getMessageId()
                    + ". The message lock will expire and the message will be redelivered.");
        }
    }

    private void settleMessage(ServiceBusReceivedMessageContext context, Map<String, String> decision, String action) {
        String messageId = context.getMessage().getMessageId();
        try {
            switch (action) {
                case ASBConstants.SETTLEMENT_ACTION_COMPLETE:
                    context.complete();
                    LOG.info("Message completed for MessageId: " + messageId);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Message completed for MessageId: " + messageId);
                    }
                    return;
                case ASBConstants.SETTLEMENT_ACTION_ABANDON:
                    context.abandon();
                    LOG.info("Message abandoned for MessageId: " + messageId);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Message abandoned for MessageId: " + messageId);
                    }
                    return;
                case ASBConstants.SETTLEMENT_ACTION_DEFER:
                    context.defer();
                    LOG.info("Message deferred for MessageId: " + messageId
                            + ", SequenceNumber: " + context.getMessage().getSequenceNumber());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Message deferred for MessageId: " + messageId
                                + ", SequenceNumber: " + context.getMessage().getSequenceNumber());
                    }
                    return;
                case ASBConstants.SETTLEMENT_ACTION_DEAD_LETTER:
                    deadLetter(context, decision);
                    return;
                default:
                    LOG.warn("Unknown settlement action '" + action + "'. MessageId: " + messageId
                            + ". The message lock will expire and the message will be redelivered.");
            }
        } catch (Exception e) {
            if (isDestroyed) {
                LOG.warn("Message settlement (" + action + ") interrupted during shutdown. "
                        + "MessageId: " + messageId
                        + ". The broker may have already processed the settlement. "
                        + "If not, the message lock will expire and the message will be redelivered.");
            } else {
                LOG.error("Error settling message. Action: " + action + ", MessageId: " + messageId, e);
            }
        }
    }

    private void deadLetter(ServiceBusReceivedMessageContext context, Map<String, String> decision) {
        String messageId = context.getMessage().getMessageId();
        String reason = decision.get(ASBConstants.DECISION_KEY_DEAD_LETTER_REASON);
        String description = decision.get(ASBConstants.DECISION_KEY_DEAD_LETTER_ERROR_DESCRIPTION);
        if (reason == null && description == null) {
            context.deadLetter();
        } else {
            DeadLetterOptions options = new DeadLetterOptions();
            if (reason != null) {
                options.setDeadLetterReason(reason);
            }
            if (description != null) {
                options.setDeadLetterErrorDescription(description);
            }
            context.deadLetter(options);
        }
        LOG.info("Message dead-lettered. MessageId: " + messageId);
    }

    private void processError(ServiceBusErrorContext context) {
        LOG.error("Error in ASB Consumer '" + name + "'. Error source: "
                        + context.getErrorSource() + ", Exception: " + context.getException().getMessage(),
                context.getException());
    }

    /**
     * Resumes the ASB listener if it has been destroyed.
     */
    public void resume() {
        if (processorClient == null || isDestroyed) {
            isDestroyed = false;
            listen();
        }
    }

    /**
     * Pauses the ASB listener by destroying its resources.
     */
    public void pause() {
        destroy();
    }

    public void destroy() {
        LOG.info("Stopping ASB Consumer: " + name);
        isDestroyed = true;

        if (processorClient != null) {
            try {
                processorClient.stop();
                LOG.info("ASB Consumer '" + name + "' stopped accepting new messages.");

                long waitStart = System.currentTimeMillis();

                // Wait up to maxLockDurationMs — beyond that the broker reclaims the message lock anyway.
                long maxWaitMs = getLongProperty(ASBConstants.MAX_LOCK_DURATION_MS, DEFAULT_MAX_LOCK_DURATION_MS);
                while (inFlightMessages.get() > 0
                        && (System.currentTimeMillis() - waitStart) < maxWaitMs) {
                    LOG.info("Waiting for " + inFlightMessages.get()
                            + " in-flight message(s) to complete...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for in-flight messages.", e);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                if (inFlightMessages.get() > 0) {
                    LOG.warn("Closing ASB Consumer '" + name + "' with "
                            + inFlightMessages.get() + " in-flight message(s) remaining.");
                }

                processorClient.close();
                LOG.info("ASB Consumer '" + name + "' closed successfully.");
            } catch (Exception e) {
                LOG.error("Error stopping ASB Consumer: " + name, e);
            }
        }
    }

    private void validateMandatoryParameters() {
        String connectionString = properties.getProperty(ASBConstants.CONNECTION_STRING);
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IllegalArgumentException("Parameter '" + ASBConstants.CONNECTION_STRING + "' is required.");
        }

        if (injectingSeq == null || injectingSeq.isEmpty()) {
            throw new IllegalArgumentException("Injecting sequence name is required for the inbound endpoint.");
        }

        String entityType = properties.getProperty(ASBConstants.ENTITY_TYPE);
        if (entityType == null || entityType.isEmpty()) {
            throw new IllegalArgumentException("Parameter '" + ASBConstants.ENTITY_TYPE + "' is required.");
        }

        if (ASBConstants.ENTITY_TYPE_QUEUE.equalsIgnoreCase(entityType)) {
            String queueName = properties.getProperty(ASBConstants.QUEUE_NAME);
            if (queueName == null || queueName.isEmpty()) {
                throw new IllegalArgumentException(
                        "Parameter '" + ASBConstants.QUEUE_NAME + "' is required when entityType is 'queue'.");
            }
        } else if (ASBConstants.ENTITY_TYPE_TOPIC.equalsIgnoreCase(entityType)) {
            String topicName = properties.getProperty(ASBConstants.TOPIC_NAME);
            String subscriptionName = properties.getProperty(ASBConstants.SUBSCRIPTION_NAME);
            if (topicName == null || topicName.isEmpty()) {
                throw new IllegalArgumentException(
                        "Parameter '" + ASBConstants.TOPIC_NAME + "' is required when entityType is 'topic'.");
            }
            if (subscriptionName == null || subscriptionName.isEmpty()) {
                throw new IllegalArgumentException(
                        "Parameter '" + ASBConstants.SUBSCRIPTION_NAME + "' is required when entityType is 'topic'.");
            }
        } else {
            throw new IllegalArgumentException(
                    "Invalid entityType: '" + entityType + "'. Must be 'queue' or 'topic'.");
        }
    }

    private ASBMessageInjector createMessageInjector() {
        String contentType = properties.getProperty(ASBConstants.CONTENT_TYPE);
        if (contentType != null) {
            contentType = contentType.trim();
            if (contentType.isEmpty()) {
                contentType = null;
            }
        }
        boolean awaitSettlement = ServiceBusReceiveMode.PEEK_LOCK.equals(getReceiveMode());

        long messageProcessingTimeoutMs = getLongProperty(
                ASBConstants.MESSAGE_PROCESSING_TIMEOUT_MS, ASBConstants.DEFAULT_MESSAGE_PROCESSING_TIMEOUT_MS);
        long maxLockRenewMs = getLongProperty(
                ASBConstants.MAX_LOCK_DURATION_MS, DEFAULT_MAX_LOCK_DURATION_MS);
        if (messageProcessingTimeoutMs > maxLockRenewMs) {
            LOG.warn("messageProcessingTimeoutMs (" + messageProcessingTimeoutMs
                    + " ms) exceeds maxLockDurationMs (" + maxLockRenewMs
                    + " ms). Capping to maxLockDurationMs.");
            messageProcessingTimeoutMs = maxLockRenewMs;
        }

        String inboundVariableName = properties.getProperty(
                ASBConstants.INBOUND_VARIABLE_NAME, ASBConstants.DEFAULT_INBOUND_VARIABLE_NAME);

        return new ASBMessageInjector(synapseEnvironment, injectingSeq, onErrorSeq, contentType, name,
                awaitSettlement, messageProcessingTimeoutMs, inboundVariableName);
    }

    private ServiceBusProcessorClient buildProcessorClient() {
        String connectionString = properties.getProperty(ASBConstants.CONNECTION_STRING);
        ServiceBusReceiveMode receiveMode = getReceiveMode();
        int prefetchCount = getIntProperty(ASBConstants.PREFETCH_COUNT, DEFAULT_PREFETCH_COUNT);
        if (prefetchCount < 0) {
            throw new IllegalArgumentException("Parameter '" + ASBConstants.PREFETCH_COUNT
                    + "' must not be negative, but was: " + prefetchCount);
        }

        ServiceBusClientBuilder clientBuilder = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .retryOptions(getRetryOptions());

        ServiceBusProcessorClient client;
        if (isSessionEnabled()) {
            int maxConcurrentSessions = getIntProperty(
                    ASBConstants.MAX_CONCURRENT_SESSIONS, DEFAULT_MAX_CONCURRENT_SESSIONS);
            int maxConcurrentConsumersPerSession = getIntProperty(
                    ASBConstants.MAX_CONCURRENT_CONSUMERS_PER_SESSION, DEFAULT_MAX_CONCURRENT_CONSUMERS_PER_SESSION);

            ServiceBusClientBuilder.ServiceBusSessionProcessorClientBuilder builder = clientBuilder
                    .sessionProcessor()
                    .receiveMode(receiveMode)
                    .prefetchCount(prefetchCount)
                    .maxConcurrentSessions(maxConcurrentSessions)
                    .maxConcurrentCalls(maxConcurrentConsumersPerSession)
                    .disableAutoComplete()
                    .processMessage(this::processMessage)
                    .processError(this::processError);

            long sessionIdleTimeoutMs = getLongProperty(
                    ASBConstants.SESSION_IDLE_TIMEOUT_MS, DEFAULT_SESSION_IDLE_TIMEOUT_MS);
            builder.sessionIdleTimeout(Duration.ofMillis(sessionIdleTimeoutMs));

            if (ServiceBusReceiveMode.PEEK_LOCK.equals(receiveMode)) {
                long lockRenewDurationMs = getLongProperty(
                        ASBConstants.MAX_LOCK_DURATION_MS, DEFAULT_MAX_LOCK_DURATION_MS);
                builder.maxAutoLockRenewDuration(Duration.ofMillis(lockRenewDurationMs));
            }
            setEntityConfig(builder);
            client = builder.buildProcessorClient();
        } else {
            int maxConcurrentConsumers = getIntProperty(
                    ASBConstants.MAX_CONCURRENT_CONSUMERS, DEFAULT_MAX_CONCURRENT_CONSUMERS);

            ServiceBusClientBuilder.ServiceBusProcessorClientBuilder builder = clientBuilder
                    .processor()
                    .receiveMode(receiveMode)
                    .prefetchCount(prefetchCount)
                    .maxConcurrentCalls(maxConcurrentConsumers)
                    .disableAutoComplete()
                    .processMessage(this::processMessage)
                    .processError(this::processError);

            if (ServiceBusReceiveMode.PEEK_LOCK.equals(receiveMode)) {
                long lockRenewDurationMs = getLongProperty(
                        ASBConstants.MAX_LOCK_DURATION_MS, DEFAULT_MAX_LOCK_DURATION_MS);
                builder.maxAutoLockRenewDuration(Duration.ofMillis(lockRenewDurationMs));
            }
            setEntityConfig(builder);
            client = builder.buildProcessorClient();
        }

        return client;
    }

    private void setEntityConfig(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder builder) {
        String entityType = properties.getProperty(ASBConstants.ENTITY_TYPE);
        if (ASBConstants.ENTITY_TYPE_QUEUE.equalsIgnoreCase(entityType)) {
            builder.queueName(properties.getProperty(ASBConstants.QUEUE_NAME));
        } else if (ASBConstants.ENTITY_TYPE_TOPIC.equalsIgnoreCase(entityType)) {
            builder.topicName(properties.getProperty(ASBConstants.TOPIC_NAME))
                    .subscriptionName(properties.getProperty(ASBConstants.SUBSCRIPTION_NAME));
        }
    }

    private void setEntityConfig(ServiceBusClientBuilder.ServiceBusSessionProcessorClientBuilder builder) {
        String entityType = properties.getProperty(ASBConstants.ENTITY_TYPE);
        if (ASBConstants.ENTITY_TYPE_QUEUE.equalsIgnoreCase(entityType)) {
            builder.queueName(properties.getProperty(ASBConstants.QUEUE_NAME));
        } else if (ASBConstants.ENTITY_TYPE_TOPIC.equalsIgnoreCase(entityType)) {
            builder.topicName(properties.getProperty(ASBConstants.TOPIC_NAME))
                    .subscriptionName(properties.getProperty(ASBConstants.SUBSCRIPTION_NAME));
        }
    }

    private boolean isSessionEnabled() {
        return Boolean.parseBoolean(properties.getProperty(ASBConstants.SESSION_ENABLED, "false"));
    }

    private ServiceBusReceiveMode getReceiveMode() {
        String mode = properties.getProperty(ASBConstants.RECEIVE_MODE, ASBConstants.RECEIVE_MODE_PEEK_LOCK);
        if (ASBConstants.RECEIVE_MODE_RECEIVE_AND_DELETE.equalsIgnoreCase(mode)) {
            return ServiceBusReceiveMode.RECEIVE_AND_DELETE;
        }
        return ServiceBusReceiveMode.PEEK_LOCK;
    }

    private AmqpRetryOptions getRetryOptions() {
        AmqpRetryOptions retryOptions = new AmqpRetryOptions();
        retryOptions.setMaxRetries(getIntProperty(ASBConstants.RETRY_MAX_RETRIES, DEFAULT_MAX_RETRIES));
        retryOptions.setDelay(Duration.ofMillis(
                getLongProperty(ASBConstants.RETRY_DELAY_MS, DEFAULT_RETRY_DELAY_MS)));
        retryOptions.setMaxDelay(Duration.ofMillis(
                getLongProperty(ASBConstants.RETRY_MAX_DELAY_MS, DEFAULT_RETRY_MAX_DELAY_MS)));
        retryOptions.setTryTimeout(Duration.ofMillis(
                getLongProperty(ASBConstants.RETRY_TRY_TIMEOUT_MS, DEFAULT_TRY_TIMEOUT_MS)));
        retryOptions.setMode(AmqpRetryMode.EXPONENTIAL);
        return retryOptions;
    }

    private int getIntProperty(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value != null && !value.isEmpty()) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer value for '" + key + "': " + value + ". Using default: " + defaultValue);
            }
        }
        return defaultValue;
    }

    private long getLongProperty(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value != null && !value.isEmpty()) {
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid long value for '" + key + "': " + value + ". Using default: " + defaultValue);
            }
        }
        return defaultValue;
    }
}
