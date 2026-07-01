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

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericConstants;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Injects Azure Service Bus messages into the MI mediation flow.
 */
public class ASBMessageInjector {

    private static final Log LOG = LogFactory.getLog(ASBMessageInjector.class);

    private final SynapseEnvironment synapseEnvironment;
    private final String injectingSequence;
    private final String onErrorSequence;
    private final String contentType;
    private final String name;
    private final boolean awaitSettlement;
    private final long settlementWaitTimeoutMs;

    public ASBMessageInjector(SynapseEnvironment synapseEnvironment, String injectingSequence,
                              String onErrorSequence, String contentType, String name,
                              boolean awaitSettlement, long settlementWaitTimeoutMs) {
        this.synapseEnvironment = synapseEnvironment;
        this.injectingSequence = injectingSequence;
        this.onErrorSequence = onErrorSequence;
        this.contentType = contentType;
        this.name = name;
        this.awaitSettlement = awaitSettlement;
        this.settlementWaitTimeoutMs = settlementWaitTimeoutMs;
    }

    public MessageContext inject(ServiceBusReceivedMessageContext sbContext) {
        ServiceBusReceivedMessage message = sbContext.getMessage();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Injecting ASB message. MessageId: " + message.getMessageId());
        }
        MessageContext synCtx = createMessageContext();
        CountDownLatch settlementLatch = null;
        if (awaitSettlement) {
            settlementLatch = new CountDownLatch(1);
            synCtx.setProperty(ASBConstants.ASB_SETTLEMENT_LATCH, settlementLatch);
            AtomicReference<Map<String, String>> atomicReference = new AtomicReference<>(new HashMap<>());
            synCtx.setProperty(ASBConstants.ASB_SETTLEMENT_DECISION, atomicReference);
        }
        setMessageProperties(synCtx, message);
        setTransportHeaders(synCtx, message);
        try {
            setMessageBody(synCtx, message);
            injectToSequence(synCtx);
        } catch (AxisFault axisFault) {
            LOG.error("Error while building the message", axisFault);
            synCtx.setProperty(SynapseConstants.ERROR_CODE, GenericConstants.INBOUND_BUILD_ERROR);
            synCtx.setProperty(SynapseConstants.ERROR_MESSAGE, axisFault.getMessage());
            SequenceMediator faultSequence = getFaultSequence(synCtx);
            if (faultSequence != null) {
                faultSequence.mediate(synCtx);
            } else {
                LOG.error("No fault sequence found to handle the error.");
            }
        }
        if (settlementLatch != null) {
            awaitSettlementDecision(settlementLatch, message);
        }
        return synCtx;
    }

    /**
     * Blocks the consumer thread until the mediation flow records a settlement decision (the
     * connector's inbound settlement mediator counts the latch down) or the wait times out.
     * On timeout the caller falls back to its configured default action.
     */
    private void awaitSettlementDecision(CountDownLatch settlementLatch, ServiceBusReceivedMessage message) {
        try {
            boolean decided = settlementLatch.await(settlementWaitTimeoutMs, TimeUnit.MILLISECONDS);
            if (!decided) {
                LOG.warn("Timed out after " + settlementWaitTimeoutMs + " ms waiting for a settlement decision. "
                        + "MessageId: " + message.getMessageId() + ". The default action will be applied.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for a settlement decision. MessageId: "
                    + message.getMessageId(), e);
        }
    }

    private MessageContext createMessageContext() {
        MessageContext synCtx = synapseEnvironment.createMessageContext();
        org.apache.axis2.context.MessageContext axis2MsgCtx =
                ((Axis2MessageContext) synCtx).getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(UUID.randomUUID().toString());
        return synCtx;
    }

    private void setMessageProperties(MessageContext synCtx, ServiceBusReceivedMessage message) {
        if (message.getTimeToLive() != null) {
            synCtx.setProperty(ASBConstants.ASB_TIME_TO_LIVE, message.getTimeToLive().toString());
        }
        synCtx.setProperty(ASBConstants.ASB_DELIVERY_COUNT, message.getDeliveryCount());
        if (message.getEnqueuedTime() != null) {
            synCtx.setProperty(ASBConstants.ASB_ENQUEUED_TIME, message.getEnqueuedTime().toString());
        }
        synCtx.setProperty(ASBConstants.ASB_SEQUENCE_NUMBER, message.getSequenceNumber());

        if (message.getPartitionKey() != null) {
            synCtx.setProperty(ASBConstants.ASB_PARTITION_KEY, message.getPartitionKey());
        }
        if (message.getTo() != null) {
            synCtx.setProperty(ASBConstants.ASB_TO, message.getTo());
        }
        if (message.getDeadLetterSource() != null) {
            synCtx.setProperty(ASBConstants.ASB_DEAD_LETTER_SOURCE, message.getDeadLetterSource());
        }
    }

    private void setTransportHeaders(MessageContext synCtx, ServiceBusReceivedMessage message) {
        org.apache.axis2.context.MessageContext axis2MsgCtx =
                ((Axis2MessageContext) synCtx).getAxis2MessageContext();
        Map<String, String> transportHeaders = new HashMap<>();

        if (message.getMessageId() != null) {
            transportHeaders.put(ASBConstants.ASB_MESSAGE_ID, message.getMessageId());
        }
        if (message.getCorrelationId() != null) {
            transportHeaders.put(ASBConstants.ASB_CORRELATION_ID, message.getCorrelationId());
        }
        if (message.getContentType() != null) {
            transportHeaders.put(ASBConstants.ASB_CONTENT_TYPE, message.getContentType());
        }
        if (message.getSubject() != null) {
            transportHeaders.put(ASBConstants.ASB_SUBJECT, message.getSubject());
        }
        if (message.getReplyTo() != null) {
            transportHeaders.put(ASBConstants.ASB_REPLY_TO, message.getReplyTo());
        }

        message.getApplicationProperties().forEach((key, value) -> {
            if (key != null && value != null) {
                transportHeaders.put(key, value.toString());
            }
        });

        axis2MsgCtx.setProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS, transportHeaders);
    }

    private void setMessageBody(MessageContext synCtx, ServiceBusReceivedMessage message) throws AxisFault {
        org.apache.axis2.context.MessageContext axis2MsgCtx =
                ((Axis2MessageContext) synCtx).getAxis2MessageContext();

        String resolvedContentType = contentType;
        if (resolvedContentType == null && message.getContentType() != null) {
            resolvedContentType = message.getContentType();
        }
        byte[] bodyBytes = message.getBody().toBytes();
        InputStream inputStream = new AutoCloseInputStream(new ByteArrayInputStream(bodyBytes));

        Builder builder;
        if (resolvedContentType == null) {
            builder = new SOAPBuilder();
        } else {
            int index = resolvedContentType.indexOf(';');
            String type = index > 0 ? resolvedContentType.substring(0, index) : resolvedContentType;
            builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
            if (builder == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No message builder found for type '" + type + "'. Falling back to SOAP builder.");
                }
                builder = new SOAPBuilder();
            }
            axis2MsgCtx.setProperty(Constants.Configuration.CONTENT_TYPE, resolvedContentType);
            axis2MsgCtx.setProperty(Constants.Configuration.MESSAGE_TYPE, type);
        }

        OMElement documentElement = builder.processDocument(inputStream, resolvedContentType, axis2MsgCtx);
        synCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
    }

    private void injectToSequence(MessageContext synCtx) {
        if (injectingSequence == null || injectingSequence.isEmpty()) {
            LOG.error("Injecting sequence name is not specified.");
            return;
        }
        SequenceMediator seq = (SequenceMediator) synapseEnvironment
                .getSynapseConfiguration().getSequence(injectingSequence);
        if (seq != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Injecting message to sequence: " + injectingSequence);
            }
            if (onErrorSequence != null && !onErrorSequence.equals(seq.getErrorHandler())) {
                seq.setErrorHandler(onErrorSequence);
            }
            synCtx.setProperty(SynapseConstants.IS_INBOUND, true);
            synCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, name);
            synCtx.setProperty(SynapseConstants.ARTIFACT_NAME,
                    SynapseConstants.FAIL_SAFE_MODE_INBOUND_ENDPOINT + name);
            synapseEnvironment.injectInbound(synCtx, seq, true);
        } else {
            LOG.error("Sequence '" + injectingSequence + "' not found.");
        }
    }

    private SequenceMediator getFaultSequence(MessageContext synCtx) {
        SequenceMediator faultSequence = null;
        if (onErrorSequence != null) {
            faultSequence = (SequenceMediator) synapseEnvironment.getSynapseConfiguration().getSequence(onErrorSequence);
        }

        if (faultSequence == null) {
            faultSequence = (SequenceMediator) synCtx.getFaultSequence();
        }

        return faultSequence;
    }
}
