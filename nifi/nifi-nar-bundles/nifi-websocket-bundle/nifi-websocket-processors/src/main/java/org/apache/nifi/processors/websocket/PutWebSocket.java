/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.websocket;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_CS_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_ENDPOINT_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_FAILURE_DETAIL;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_LOCAL_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_MESSAGE_TYPE;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_REMOTE_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_SESSION_ID;
import static org.apache.nifi.websocket.WebSocketMessage.CHARSET_NAME;

@Tags({"WebSocket", "publish", "send"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@TriggerSerially
@CapabilityDescription("Sends messages to a WebSocket remote endpoint" +
        " using a WebSocket session that is established by either ListenWebSocket or ConnectWebSocket.")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_WS_CS_ID, description = "WebSocket Controller Service id."),
        @WritesAttribute(attribute = ATTR_WS_SESSION_ID, description = "Established WebSocket session id."),
        @WritesAttribute(attribute = ATTR_WS_ENDPOINT_ID, description = "WebSocket endpoint id."),
        @WritesAttribute(attribute = ATTR_WS_MESSAGE_TYPE, description = "TEXT or BINARY."),
        @WritesAttribute(attribute = ATTR_WS_LOCAL_ADDRESS, description = "WebSocket server address."),
        @WritesAttribute(attribute = ATTR_WS_REMOTE_ADDRESS, description = "WebSocket client address."),
        @WritesAttribute(attribute = ATTR_WS_FAILURE_DETAIL, description = "Detail of the failure."),
})
public class PutWebSocket extends AbstractProcessor {

    public static final PropertyDescriptor PROP_WS_SESSION_ID = new PropertyDescriptor.Builder()
            .name("websocket-session-id")
            .displayName("WebSocket Session Id")
            .description("A NiFi Expression to retrieve the session id.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("${" + ATTR_WS_SESSION_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_CONTROLLER_SERVICE_ID = new PropertyDescriptor.Builder()
            .name("websocket-controller-service-id")
            .displayName("WebSocket ControllerService Id")
            .description("A NiFi Expression to retrieve the id of a WebSocket ControllerService.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("${" + ATTR_WS_CS_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_CONTROLLER_SERVICE_ENDPOINT = new PropertyDescriptor.Builder()
            .name("websocket-endpoint-id")
            .displayName("WebSocket Endpoint Id")
            .description("A NiFi Expression to retrieve the endpoint id of a WebSocket ControllerService.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("${" + ATTR_WS_ENDPOINT_ID + "}")
            .build();

    public static final PropertyDescriptor PROP_WS_MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("websocket-message-type")
            .displayName("WebSocket Message Type")
            .description("The type of message content: TEXT or BINARY")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(WebSocketMessage.Type.TEXT.toString())
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are transferred to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are transferred to this relationship.")
            .build();

    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;

    static{
        final List<PropertyDescriptor> innerDescriptorsList = new ArrayList<>();
        innerDescriptorsList.add(PROP_WS_SESSION_ID);
        innerDescriptorsList.add(PROP_WS_CONTROLLER_SERVICE_ID);
        innerDescriptorsList.add(PROP_WS_CONTROLLER_SERVICE_ENDPOINT);
        innerDescriptorsList.add(PROP_WS_MESSAGE_TYPE);
        descriptors = Collections.unmodifiableList(innerDescriptorsList);

        final Set<Relationship> innerRelationshipsSet = new HashSet<>();
        innerRelationshipsSet.add(REL_SUCCESS);
        innerRelationshipsSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(innerRelationshipsSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) throws ProcessException {
        final FlowFile flowfile = processSession.get();
        if (flowfile == null) {
            return;
        }

        final String sessionId = context.getProperty(PROP_WS_SESSION_ID)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String webSocketServiceId = context.getProperty(PROP_WS_CONTROLLER_SERVICE_ID)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String webSocketServiceEndpoint = context.getProperty(PROP_WS_CONTROLLER_SERVICE_ENDPOINT)
                .evaluateAttributeExpressions(flowfile).getValue();
        final String messageTypeStr = context.getProperty(PROP_WS_MESSAGE_TYPE)
                .evaluateAttributeExpressions(flowfile).getValue();
        final WebSocketMessage.Type messageType = WebSocketMessage.Type.valueOf(messageTypeStr);

        if (StringUtils.isEmpty(sessionId)
                || StringUtils.isEmpty(webSocketServiceId)
                || StringUtils.isEmpty(webSocketServiceEndpoint)) {
            transferToFailure(processSession, flowfile, "Required WebSocket attribute was not found.");
            return;
        }

        final ControllerService controllerService = context.getControllerServiceLookup().getControllerService(webSocketServiceId);
        if (controllerService == null) {
            transferToFailure(processSession, flowfile, "WebSocket ControllerService was not found.");
            return;
        } else if (!(controllerService instanceof WebSocketService)) {
            transferToFailure(processSession, flowfile, "The ControllerService found was not a WebSocket ControllerService but a "
                    + controllerService.getClass().getName());
            return;
        }

        final WebSocketService webSocketService = (WebSocketService)controllerService;
        final byte[] messageContent = new byte[(int) flowfile.getSize()];
        final long startSending = System.currentTimeMillis();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(ATTR_WS_CS_ID, webSocketService.getIdentifier());
        attrs.put(ATTR_WS_SESSION_ID, sessionId);
        attrs.put(ATTR_WS_ENDPOINT_ID, webSocketServiceEndpoint);
        attrs.put(ATTR_WS_MESSAGE_TYPE, messageTypeStr);

        processSession.read(flowfile, in -> {
            StreamUtils.fillBuffer(in, messageContent, true);
        });

        try {

            webSocketService.sendMessage(webSocketServiceEndpoint, sessionId, sender -> {
                switch (messageType) {
                    case TEXT:
                        sender.sendString(new String(messageContent, CHARSET_NAME));
                        break;
                    case BINARY:
                        sender.sendBinary(ByteBuffer.wrap(messageContent));
                        break;
                }

                attrs.put(ATTR_WS_LOCAL_ADDRESS, sender.getLocalAddress().toString());
                attrs.put(ATTR_WS_REMOTE_ADDRESS, sender.getRemoteAddress().toString());

                final FlowFile updatedFlowFile = processSession.putAllAttributes(flowfile, attrs);
                final long transmissionMillis = System.currentTimeMillis() - startSending;
                processSession.getProvenanceReporter().send(updatedFlowFile, sender.getTransitUri(), transmissionMillis);

                processSession.transfer(updatedFlowFile, REL_SUCCESS);
            });

        } catch (WebSocketConfigurationException|IllegalStateException|IOException e) {
            // WebSocketConfigurationException: If the corresponding WebSocketGatewayProcessor has been stopped.
            // IllegalStateException: Session is already closed or not found.
            // IOException: other IO error.
            getLogger().error("Failed to send message via WebSocket due to " + e, e);
            transferToFailure(processSession, flowfile, e.toString());
        }

    }

    private FlowFile transferToFailure(final ProcessSession processSession, FlowFile flowfile, final String value) {
        flowfile = processSession.putAttribute(flowfile, ATTR_WS_FAILURE_DETAIL, value);
        processSession.transfer(flowfile, REL_FAILURE);
        return flowfile;
    }


}
