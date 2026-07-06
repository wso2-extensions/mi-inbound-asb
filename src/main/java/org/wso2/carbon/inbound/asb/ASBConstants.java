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

public final class ASBConstants {

    private ASBConstants() {
    }

    // Connection parameters
    public static final String CONNECTION_STRING = "connectionString";
    public static final String ENTITY_TYPE = "entityType";
    public static final String QUEUE_NAME = "queueName";
    public static final String TOPIC_NAME = "topicName";
    public static final String SUBSCRIPTION_NAME = "subscriptionName";

    // Consumer parameters
    public static final String RECEIVE_MODE = "receiveMode";
    public static final String PREFETCH_COUNT = "prefetchCount";
    public static final String MAX_LOCK_DURATION_MS = "maxLockDurationMs";
    public static final String MAX_CONCURRENT_MESSAGES = "maxConcurrentMessages";

    // Retry parameters
    public static final String RETRY_MAX_RETRIES = "maxRetries";
    public static final String RETRY_DELAY_MS = "retryDelayMs";
    public static final String RETRY_MAX_DELAY_MS = "retryMaxDelayMs";
    public static final String RETRY_TRY_TIMEOUT_MS = "tryTimeoutMs";

    // Entity type values
    public static final String ENTITY_TYPE_QUEUE = "queue";
    public static final String ENTITY_TYPE_TOPIC = "topic";

    // Receive mode values
    public static final String RECEIVE_MODE_PEEK_LOCK = "PEEK_LOCK";
    public static final String RECEIVE_MODE_RECEIVE_AND_DELETE = "RECEIVE_AND_DELETE";

    // Message context properties
    public static final String ASB_MESSAGE_ID = "asb.messageId";
    public static final String ASB_CORRELATION_ID = "asb.correlationId";
    public static final String ASB_CONTENT_TYPE = "asb.contentType";
    public static final String ASB_SUBJECT = "asb.subject";
    public static final String ASB_TO = "asb.to";
    public static final String ASB_REPLY_TO = "asb.replyTo";
    public static final String ASB_PARTITION_KEY = "asb.partitionKey";
    public static final String ASB_TIME_TO_LIVE = "asb.timeToLive";
    public static final String ASB_DELIVERY_COUNT = "asb.deliveryCount";
    public static final String ASB_ENQUEUED_TIME = "asb.enqueuedTime";
    public static final String ASB_SEQUENCE_NUMBER = "asb.sequenceNumber";
    public static final String ASB_DEAD_LETTER_SOURCE = "asb.deadLetterSource";

    // Structured message variable
    public static final String INBOUND_VARIABLE_NAME = "inboundVariableName";
    public static final String DEFAULT_INBOUND_VARIABLE_NAME = "asb_inbound";
    public static final String ASB_INBOUND_ATTRIBUTES = "attributes";
    public static final String ASB_INBOUND_HEADERS = "headers";

    // Inbound settlement handshake
    public static final String ASB_INBOUND_SETTLEMENT_LATCH = "_ASB_INBOUND_SETTLEMENT_LATCH";
    public static final String ASB_INBOUND_SETTLEMENT_DECISION = "_ASB_INBOUND_SETTLEMENT_DECISION";

    // Keys within the decision map carried by the AtomicReference holder (must match the
    // connector's Constants.DECISION_KEY_* values).
    public static final String DECISION_KEY_ACTION = "action";
    public static final String DECISION_KEY_DEAD_LETTER_REASON = "deadLetterReason";
    public static final String DECISION_KEY_DEAD_LETTER_ERROR_DESCRIPTION = "deadLetterErrorDescription";

    // Settlement action values stored under DECISION_KEY_ACTION (lower camel case).
    public static final String SETTLEMENT_ACTION_COMPLETE = "complete";
    public static final String SETTLEMENT_ACTION_ABANDON = "abandon";
    public static final String SETTLEMENT_ACTION_DEFER = "defer";
    public static final String SETTLEMENT_ACTION_DEAD_LETTER = "deadLetter";

    // Max time (ms) to wait for the message to be fully processed before timing out.
    public static final String MESSAGE_PROCESSING_TIMEOUT_MS = "messageProcessingTimeoutMs";
    public static final long DEFAULT_MESSAGE_PROCESSING_TIMEOUT_MS = 240000;

    // Content type
    public static final String CONTENT_TYPE = "contentType";
}
