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

package org.apache.nifi.cluster.coordination.node;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.HttpResponseMerger;
import org.apache.nifi.cluster.coordination.http.StandardHttpResponseMerger;
import org.apache.nifi.cluster.coordination.http.replication.RequestCompletionCallback;
import org.apache.nifi.cluster.event.Event;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.ComponentRevision;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.web.revision.RevisionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NodeClusterCoordinator implements ClusterCoordinator, ProtocolHandler, RequestCompletionCallback {
    private static final Logger logger = LoggerFactory.getLogger(NodeClusterCoordinator.class);
    private static final String EVENT_CATEGORY = "Clustering";

    private static final Pattern COUNTER_URI_PATTERN = Pattern.compile("/nifi-api/controller/counters/[a-f0-9\\-]{36}");

    private final String instanceId = UUID.randomUUID().toString();
    private volatile NodeIdentifier nodeId;

    private final ClusterCoordinationProtocolSenderListener senderListener;
    private final EventReporter eventReporter;
    private final ClusterNodeFirewall firewall;
    private final RevisionManager revisionManager;
    private volatile FlowService flowService;
    private volatile boolean connected;

    private final ConcurrentMap<NodeIdentifier, NodeConnectionStatus> nodeStatuses = new ConcurrentHashMap<>();
    private final ConcurrentMap<NodeIdentifier, CircularFifoQueue<NodeEvent>> nodeEvents = new ConcurrentHashMap<>();

    public NodeClusterCoordinator(final ClusterCoordinationProtocolSenderListener senderListener,
        final EventReporter eventReporter, final ClusterNodeFirewall firewall, final RevisionManager revisionManager) {
        this.senderListener = senderListener;
        this.flowService = null;
        this.eventReporter = eventReporter;
        this.firewall = firewall;
        this.revisionManager = revisionManager;
        senderListener.addHandler(this);
    }

    @Override
    public void shutdown() {
        final NodeConnectionStatus shutdownStatus = new NodeConnectionStatus(getLocalNodeIdentifier(), DisconnectionCode.NODE_SHUTDOWN);
        notifyOthersOfNodeStatusChange(shutdownStatus);
        logger.info("Successfully notified other nodes that I am shutting down");
    }

    @Override
    public void setLocalNodeIdentifier(final NodeIdentifier nodeId) {
        this.nodeId = nodeId;
    }

    NodeIdentifier getLocalNodeIdentifier() {
        return nodeId;
    }

    @Override
    public void resetNodeStatuses(final Map<NodeIdentifier, NodeConnectionStatus> statusMap) {
        logger.info("Resetting cluster node statuses from {} to {}", nodeStatuses, statusMap);

        // For each proposed replacement, update the nodeStatuses map if and only if the replacement
        // has a larger update id than the current value.
        for (final Map.Entry<NodeIdentifier, NodeConnectionStatus> entry : statusMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final NodeConnectionStatus proposedStatus = entry.getValue();

            boolean updated = false;
            while (!updated) {
                final NodeConnectionStatus currentStatus = nodeStatuses.get(nodeId);

                if (currentStatus == null || proposedStatus.getUpdateIdentifier() > currentStatus.getUpdateIdentifier()) {
                    updated = replaceNodeStatus(nodeId, currentStatus, proposedStatus);
                } else {
                    updated = true;
                }
            }
        }
    }

    /**
     * Attempts to update the nodeStatuses map by changing the value for the given node id from the current status to the new status, as in
     * ConcurrentMap.replace(nodeId, currentStatus, newStatus) but with the difference that this method can handle a <code>null</code> value
     * for currentStatus
     *
     * @param nodeId the node id
     * @param currentStatus the current status, or <code>null</code> if there is no value currently
     * @param newStatus the new status to set
     * @return <code>true</code> if the map was updated, false otherwise
     */
    private boolean replaceNodeStatus(final NodeIdentifier nodeId, final NodeConnectionStatus currentStatus, final NodeConnectionStatus newStatus) {
        if (newStatus == null) {
            logger.error("Cannot change node status for {} from {} to {} because new status is null", nodeId, currentStatus, newStatus);
            logger.error("", new NullPointerException());
        }

        if (currentStatus == null) {
            final NodeConnectionStatus existingValue = nodeStatuses.putIfAbsent(nodeId, newStatus);
            return existingValue == null;
        }

        return nodeStatuses.replace(nodeId, currentStatus, newStatus);
    }

    @Override
    public void requestNodeConnect(final NodeIdentifier nodeId, final String userDn) {
        if (userDn == null) {
            reportEvent(nodeId, Severity.INFO, "Requesting that node connect to cluster");
        } else {
            reportEvent(nodeId, Severity.INFO, "Requesting that node connect to cluster on behalf of " + userDn);
        }

        updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTING, System.currentTimeMillis()));

        // create the request
        final ReconnectionRequestMessage request = new ReconnectionRequestMessage();
        request.setNodeId(nodeId);
        request.setInstanceId(instanceId);

        requestReconnectionAsynchronously(request, 10, 5);
    }

    @Override
    public void finishNodeConnection(final NodeIdentifier nodeId) {
        final NodeConnectionState state = getConnectionState(nodeId);
        if (state == null) {
            logger.debug("Attempted to finish node connection for {} but node is not known. Requesting that node connect", nodeId);
            requestNodeConnect(nodeId, null);
            return;
        }

        if (state == NodeConnectionState.CONNECTED) {
            // already connected. Nothing to do.
            return;
        }

        if (state == NodeConnectionState.DISCONNECTED || state == NodeConnectionState.DISCONNECTING) {
            logger.debug("Attempted to finish node connection for {} but node state was {}. Requesting that node connect", nodeId, state);
            requestNodeConnect(nodeId, null);
            return;
        }

        logger.info("{} is now connected", nodeId);
        final boolean updated = updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED));
        if (!updated) {
            logger.error("Attempting to Finish Node Connection but could not find Node with Identifier {}", nodeId);
        }
    }

    @Override
    public void requestNodeDisconnect(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        logger.info("Requesting that {} disconnect due to {}", nodeId, explanation == null ? disconnectionCode : explanation);

        updateNodeStatus(new NodeConnectionStatus(nodeId, disconnectionCode, explanation));

        // There is no need to tell the node that it's disconnected if it is due to being
        // shutdown, as we will not be able to connect to the node anyway.
        if (disconnectionCode == DisconnectionCode.NODE_SHUTDOWN) {
            return;
        }

        final DisconnectMessage request = new DisconnectMessage();
        request.setNodeId(nodeId);
        request.setExplanation(explanation);

        addNodeEvent(nodeId, "Disconnection requested due to " + explanation);
        disconnectAsynchronously(request, 10, 5);
    }

    @Override
    public void disconnectionRequestedByNode(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        logger.info("{} requested disconnection from cluster due to {}", nodeId, explanation == null ? disconnectionCode : explanation);
        updateNodeStatus(new NodeConnectionStatus(nodeId, disconnectionCode, explanation));

        final Severity severity;
        switch (disconnectionCode) {
            case STARTUP_FAILURE:
            case MISMATCHED_FLOWS:
            case UNKNOWN:
                severity = Severity.ERROR;
                break;
            default:
                severity = Severity.INFO;
                break;
        }

        reportEvent(nodeId, severity, "Node disconnected from cluster due to " + explanation);
    }

    @Override
    public void removeNode(final NodeIdentifier nodeId, final String userDn) {
        reportEvent(nodeId, Severity.INFO, "User " + userDn + " requested that node be removed from cluster");
        nodeStatuses.remove(nodeId);
        nodeEvents.remove(nodeId);
        notifyOthersOfNodeStatusChange(new NodeConnectionStatus(nodeId, NodeConnectionState.REMOVED));
    }

    @Override
    public NodeConnectionStatus getConnectionStatus(final NodeIdentifier nodeId) {
        return nodeStatuses.get(nodeId);
    }

    private NodeConnectionState getConnectionState(final NodeIdentifier nodeId) {
        final NodeConnectionStatus status = getConnectionStatus(nodeId);
        return status == null ? null : status.getState();
    }


    @Override
    public Map<NodeConnectionState, List<NodeIdentifier>> getConnectionStates() {
        final Map<NodeConnectionState, List<NodeIdentifier>> connectionStates = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, NodeConnectionStatus> entry : nodeStatuses.entrySet()) {
            final NodeConnectionState state = entry.getValue().getState();
            final List<NodeIdentifier> nodeIds = connectionStates.computeIfAbsent(state, s -> new ArrayList<NodeIdentifier>());
            nodeIds.add(entry.getKey());
        }

        return connectionStates;
    }

    @Override
    public boolean isBlockedByFirewall(final String hostname) {
        return firewall != null && !firewall.isPermissible(hostname);
    }

    @Override
    public void reportEvent(final NodeIdentifier nodeId, final Severity severity, final String event) {
        eventReporter.reportEvent(severity, EVENT_CATEGORY, nodeId == null ? event : "Event Reported for " + nodeId + " -- " + event);
        if (nodeId != null) {
            addNodeEvent(nodeId, severity, event);
        }

        final String message = nodeId == null ? event : "Event Reported for " + nodeId + " -- " + event;
        switch (severity) {
            case ERROR:
                logger.error(message);
                break;
            case WARNING:
                logger.warn(message);
                break;
            case INFO:
                logger.info(message);
                break;
        }
    }

    @Override
    public synchronized void updateNodeRoles(final NodeIdentifier nodeId, final Set<String> roles) {
        boolean updated = false;
        while (!updated) {
            final NodeConnectionStatus currentStatus = nodeStatuses.get(nodeId);
            if (currentStatus == null) {
                throw new IllegalStateException("Cannot update roles for " + nodeId + " to " + roles + " because the node is not part of this cluster");
            }

            if (currentStatus.getRoles().equals(roles)) {
                logger.debug("Roles for {} already up-to-date as {}", nodeId, roles);
                return;
            }

            final NodeConnectionStatus updatedStatus = new NodeConnectionStatus(currentStatus, roles);
            updated = replaceNodeStatus(nodeId, currentStatus, updatedStatus);

            if (updated) {
                logger.info("Updated Roles of {} from {} to {}", nodeId, currentStatus, updatedStatus);
                notifyOthersOfNodeStatusChange(updatedStatus);
            }
        }

        // If any other node contains any of the given roles, revoke the role from the other node.
        for (final String role : roles) {
            for (final Map.Entry<NodeIdentifier, NodeConnectionStatus> entry : nodeStatuses.entrySet()) {
                if (entry.getKey().equals(nodeId)) {
                    continue;
                }

                updated = false;
                while (!updated) {
                    final NodeConnectionStatus status = entry.getValue();
                    if (status.getRoles().contains(role)) {
                        final Set<String> newRoles = new HashSet<>(status.getRoles());
                        newRoles.remove(role);

                        final NodeConnectionStatus updatedStatus = new NodeConnectionStatus(status, newRoles);
                        updated = replaceNodeStatus(entry.getKey(), status, updatedStatus);

                        if (updated) {
                            logger.info("Updated Roles of {} from {} to {}", nodeId, status, updatedStatus);
                            notifyOthersOfNodeStatusChange(updatedStatus);
                        }
                    } else {
                        updated = true;
                    }
                }
            }
        }
    }

    @Override
    public NodeIdentifier getNodeIdentifier(final String uuid) {
        for (final NodeIdentifier nodeId : nodeStatuses.keySet()) {
            if (nodeId.getId().equals(uuid)) {
                return nodeId;
            }
        }

        return null;
    }


    @Override
    public Set<NodeIdentifier> getNodeIdentifiers(final NodeConnectionState... states) {
        final Set<NodeConnectionState> statesOfInterest = new HashSet<>();
        if (states.length == 0) {
            for (final NodeConnectionState state : NodeConnectionState.values()) {
                statesOfInterest.add(state);
            }
        } else {
            for (final NodeConnectionState state : states) {
                statesOfInterest.add(state);
            }
        }

        return nodeStatuses.entrySet().stream()
            .filter(entry -> statesOfInterest.contains(entry.getValue().getState()))
            .map(entry -> entry.getKey())
            .collect(Collectors.toSet());
    }

    @Override
    public NodeIdentifier getPrimaryNode() {
        return nodeStatuses.values().stream()
            .filter(status -> status.getRoles().contains(ClusterRoles.PRIMARY_NODE))
            .findFirst()
            .map(status -> status.getNodeIdentifier())
            .orElse(null);
    }

    @Override
    public List<NodeEvent> getNodeEvents(final NodeIdentifier nodeId) {
        final CircularFifoQueue<NodeEvent> eventQueue = nodeEvents.get(nodeId);
        if (eventQueue == null) {
            return Collections.emptyList();
        }

        synchronized (eventQueue) {
            return new ArrayList<>(eventQueue);
        }
    }

    @Override
    public void setFlowService(final FlowService flowService) {
        if (this.flowService != null) {
            throw new IllegalStateException("Flow Service has already been set");
        }
        this.flowService = flowService;
    }

    private void addNodeEvent(final NodeIdentifier nodeId, final String event) {
        addNodeEvent(nodeId, Severity.INFO, event);
    }

    private void addNodeEvent(final NodeIdentifier nodeId, final Severity severity, final String message) {
        final NodeEvent event = new Event(nodeId.toString(), message, severity);
        final CircularFifoQueue<NodeEvent> eventQueue = nodeEvents.computeIfAbsent(nodeId, id -> new CircularFifoQueue<>());
        synchronized (eventQueue) {
            eventQueue.add(event);
        }
    }

    /**
     * Updates the status of the node with the given ID to the given status and returns <code>true</code>
     * if successful, <code>false</code> if no node exists with the given ID
     *
     * @param status the new status of the node
     * @return <code>true</code> if the node exists and is updated, <code>false</code> if the node does not exist
     */
    // visible for testing.
    boolean updateNodeStatus(final NodeConnectionStatus status) {
        final NodeIdentifier nodeId = status.getNodeIdentifier();

        // In this case, we are using nodeStatuses.put() instead of getting the current value and
        // comparing that to the new value and using the one with the largest update id. This is because
        // this method is called when something occurs that causes this node to change the status of the
        // node in question. We only use comparisons against the current value when we receive an update
        // about a node status from a different node, since those may be received out-of-order.
        final NodeConnectionStatus currentStatus = nodeStatuses.put(nodeId, status);
        final NodeConnectionState currentState = currentStatus == null ? null : currentStatus.getState();
        logger.info("Status of {} changed from {} to {}", nodeId, currentStatus, status);
        logger.debug("State of cluster nodes is now {}", nodeStatuses);

        if (currentState == null || currentState != status.getState()) {
            notifyOthersOfNodeStatusChange(status);
        }

        return true;
    }


    private void notifyOthersOfNodeStatusChange(final NodeConnectionStatus updatedStatus) {
        final Set<NodeIdentifier> nodesToNotify = getNodeIdentifiers(NodeConnectionState.CONNECTED, NodeConnectionState.CONNECTING);

        // Do not notify ourselves because we already know about the status update.
        nodesToNotify.remove(getLocalNodeIdentifier());

        final NodeStatusChangeMessage message = new NodeStatusChangeMessage();
        message.setNodeId(updatedStatus.getNodeIdentifier());
        message.setNodeConnectionStatus(updatedStatus);
        senderListener.notifyNodeStatusChange(nodesToNotify, message);
    }

    private void disconnectAsynchronously(final DisconnectMessage request, final int attempts, final int retrySeconds) {
        final Thread disconnectThread = new Thread(new Runnable() {
            @Override
            public void run() {
                final NodeIdentifier nodeId = request.getNodeId();

                for (int i = 0; i < attempts; i++) {
                    try {
                        senderListener.disconnect(request);
                        reportEvent(nodeId, Severity.INFO, "Node disconnected due to " + request.getExplanation());
                        return;
                    } catch (final Exception e) {
                        logger.error("Failed to notify {} that it has been disconnected from the cluster due to {}", request.getNodeId(), request.getExplanation());

                        try {
                            Thread.sleep(retrySeconds * 1000L);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        }, "Disconnect " + request.getNodeId());

        disconnectThread.start();
    }

    private void requestReconnectionAsynchronously(final ReconnectionRequestMessage request, final int reconnectionAttempts, final int retrySeconds) {
        final Thread reconnectionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // create the request
                while (flowService == null) {
                    try {
                        Thread.sleep(100L);
                    } catch (final InterruptedException ie) {
                        logger.info("Could not send Reconnection request to {} because thread was "
                            + "interrupted before FlowService was made available", request.getNodeId());
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                for (int i = 0; i < reconnectionAttempts; i++) {
                    try {
                        if (NodeConnectionState.CONNECTING != getConnectionState(request.getNodeId())) {
                            // the node status has changed. It's no longer appropriate to attempt reconnection.
                            return;
                        }

                        request.setDataFlow(new StandardDataFlow(flowService.createDataFlow()));
                        request.setNodeConnectionStatuses(new ArrayList<>(nodeStatuses.values()));
                        request.setComponentRevisions(revisionManager.getAllRevisions().stream().map(rev -> ComponentRevision.fromRevision(rev)).collect(Collectors.toList()));

                        // Issue a reconnection request to the node.
                        senderListener.requestReconnection(request);

                        // successfully told node to reconnect -- we're done!
                        logger.info("Successfully requested that {} join the cluster", request.getNodeId());

                        return;
                    } catch (final Exception e) {
                        logger.warn("Problem encountered issuing reconnection request to node " + request.getNodeId(), e);
                        eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Problem encountered issuing reconnection request to node "
                            + request.getNodeId() + " due to: " + e);
                    }

                    try {
                        Thread.sleep(1000L * retrySeconds);
                    } catch (final InterruptedException ie) {
                        break;
                    }
                }

                // We failed to reconnect too many times. We must now mark node as disconnected.
                if (NodeConnectionState.CONNECTING == getConnectionState(request.getNodeId())) {
                    requestNodeDisconnect(request.getNodeId(), DisconnectionCode.UNABLE_TO_COMMUNICATE,
                        "Attempted to request that node reconnect to cluster but could not communicate with node");
                }
            }
        }, "Reconnect " + request.getNodeId());

        reconnectionThread.start();
    }

    @Override
    public ProtocolMessage handle(final ProtocolMessage protocolMessage) throws ProtocolException {
        switch (protocolMessage.getType()) {
            case CONNECTION_REQUEST:
                return handleConnectionRequest((ConnectionRequestMessage) protocolMessage);
            case NODE_STATUS_CHANGE:
                handleNodeStatusChange((NodeStatusChangeMessage) protocolMessage);
                return null;
            default:
                throw new ProtocolException("Cannot handle Protocol Message " + protocolMessage + " because it is not of the correct type");
        }
    }

    private void handleNodeStatusChange(final NodeStatusChangeMessage statusChangeMessage) {
        final NodeConnectionStatus updatedStatus = statusChangeMessage.getNodeConnectionStatus();
        final NodeIdentifier nodeId = statusChangeMessage.getNodeId();
        logger.debug("Handling request {}", statusChangeMessage);

        boolean updated = false;
        while (!updated) {
            final NodeConnectionStatus oldStatus = nodeStatuses.get(statusChangeMessage.getNodeId());
            if (oldStatus == null || updatedStatus.getUpdateIdentifier() >= oldStatus.getUpdateIdentifier()) {
                // Either remove the value from the map or update the map depending on the connection state
                if (statusChangeMessage.getNodeConnectionStatus().getState() == NodeConnectionState.REMOVED) {
                    updated = nodeStatuses.remove(nodeId, oldStatus);
                } else {
                    updated = replaceNodeStatus(nodeId, oldStatus, updatedStatus);
                }

                if (updated) {
                    logger.info("Status of {} changed from {} to {}", statusChangeMessage.getNodeId(), oldStatus, updatedStatus);

                    final NodeConnectionStatus status = statusChangeMessage.getNodeConnectionStatus();
                    final StringBuilder sb = new StringBuilder();
                    sb.append("Connection Status changed to ").append(status.getState().toString());
                    if (status.getDisconnectReason() != null) {
                        sb.append(" due to ").append(status.getDisconnectReason());
                    } else if (status.getDisconnectCode() != null) {
                        sb.append(" due to ").append(status.getDisconnectCode().toString());
                    }
                    addNodeEvent(nodeId, sb.toString());

                    // Update our counter so that we are in-sync with the cluster on the
                    // most up-to-date version of the NodeConnectionStatus' Update Identifier.
                    // We do this so that we can accurately compare status updates that are generated
                    // locally against those generated from other nodes in the cluster.
                    NodeConnectionStatus.updateIdGenerator(updatedStatus.getUpdateIdentifier());
                }
            } else {
                updated = true;
                logger.info("Received Node Status update that indicates that {} should change to {} but disregarding because the current state of {} is newer",
                    nodeId, updatedStatus, oldStatus);
            }
        }
    }

    private NodeIdentifier resolveNodeId(final NodeIdentifier proposedIdentifier) {
        final NodeConnectionStatus proposedConnectionStatus = new NodeConnectionStatus(proposedIdentifier, DisconnectionCode.NOT_YET_CONNECTED);
        final NodeConnectionStatus existingStatus = nodeStatuses.putIfAbsent(proposedIdentifier, proposedConnectionStatus);

        NodeIdentifier resolvedNodeId = proposedIdentifier;
        if (existingStatus == null) {
            // there is no node with that ID
            resolvedNodeId = proposedIdentifier;
            logger.debug("No existing node with ID {}; resolved node ID is as-proposed", proposedIdentifier.getId());
        } else if (existingStatus.getNodeIdentifier().logicallyEquals(proposedIdentifier)) {
            // there is a node with that ID but it's the same node.
            resolvedNodeId = proposedIdentifier;
            logger.debug("No existing node with ID {}; resolved node ID is as-proposed", proposedIdentifier.getId());
        } else {
            // there is a node with that ID and it's a different node
            resolvedNodeId = new NodeIdentifier(UUID.randomUUID().toString(), proposedIdentifier.getApiAddress(), proposedIdentifier.getApiPort(),
                proposedIdentifier.getSocketAddress(), proposedIdentifier.getSocketPort(), proposedIdentifier.getSiteToSiteAddress(),
                proposedIdentifier.getSiteToSitePort(), proposedIdentifier.getSiteToSiteHttpApiPort(), proposedIdentifier.isSiteToSiteSecure());
            logger.debug("A node already exists with ID {}. Proposed Node Identifier was {}; existing Node Identifier is {}; Resolved Node Identifier is {}",
                proposedIdentifier.getId(), proposedIdentifier, getNodeIdentifier(proposedIdentifier.getId()), resolvedNodeId);
        }

        return resolvedNodeId;
    }

    private ConnectionResponseMessage handleConnectionRequest(final ConnectionRequestMessage requestMessage) {
        final NodeIdentifier proposedIdentifier = requestMessage.getConnectionRequest().getProposedNodeIdentifier();
        final ConnectionRequest requestWithDn = new ConnectionRequest(addRequestorDn(proposedIdentifier, requestMessage.getRequestorDN()));

        // Resolve Node identifier.
        final NodeIdentifier resolvedNodeId = resolveNodeId(proposedIdentifier);
        final ConnectionResponse response = createConnectionResponse(requestWithDn, resolvedNodeId);
        final ConnectionResponseMessage responseMessage = new ConnectionResponseMessage();
        responseMessage.setConnectionResponse(response);
        return responseMessage;
    }

    private ConnectionResponse createConnectionResponse(final ConnectionRequest request, final NodeIdentifier resolvedNodeIdentifier) {
        if (isBlockedByFirewall(resolvedNodeIdentifier.getSocketAddress())) {
            // if the socket address is not listed in the firewall, then return a null response
            logger.info("Firewall blocked connection request from node " + resolvedNodeIdentifier);
            return ConnectionResponse.createBlockedByFirewallResponse();
        }

        // Set node's status to 'CONNECTING'
        NodeConnectionStatus status = getConnectionStatus(resolvedNodeIdentifier);
        if (status == null) {
            addNodeEvent(resolvedNodeIdentifier, "Connection requested from new node.  Setting status to connecting.");
        } else {
            addNodeEvent(resolvedNodeIdentifier, "Connection requested from existing node.  Setting status to connecting");
        }

        status = new NodeConnectionStatus(resolvedNodeIdentifier, NodeConnectionState.CONNECTING, System.currentTimeMillis());
        updateNodeStatus(status);

        DataFlow dataFlow = null;
        if (flowService != null) {
            try {
                dataFlow = flowService.createDataFlow();
            } catch (final IOException ioe) {
                logger.error("Unable to obtain current dataflow from FlowService in order to provide the flow to "
                    + resolvedNodeIdentifier + ". Will tell node to try again later", ioe);
            }
        }

        if (dataFlow == null) {
            // Create try-later response based on flow retrieval delay to give
            // the flow management service a chance to retrieve a current flow
            final int tryAgainSeconds = 5;
            addNodeEvent(resolvedNodeIdentifier, Severity.WARNING, "Connection requested from node, but manager was unable to obtain current flow. "
                + "Instructing node to try again in " + tryAgainSeconds + " seconds.");

            // return try later response
            return new ConnectionResponse(tryAgainSeconds);
        }

        // TODO: Remove the 'null' values here from the ConnectionResponse all together. These
        // will no longer be needed for site-to-site once the NCM is gone.
        return new ConnectionResponse(resolvedNodeIdentifier, dataFlow, null, null, null, instanceId,
            new ArrayList<>(nodeStatuses.values()),
            revisionManager.getAllRevisions().stream().map(rev -> ComponentRevision.fromRevision(rev)).collect(Collectors.toList()));
    }

    private NodeIdentifier addRequestorDn(final NodeIdentifier nodeId, final String dn) {
        return new NodeIdentifier(nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort(),
            nodeId.getSocketAddress(), nodeId.getSocketPort(),
            nodeId.getSiteToSiteAddress(), nodeId.getSiteToSitePort(),
            nodeId.getSiteToSiteHttpApiPort(), nodeId.isSiteToSiteSecure(), dn);
    }

    @Override
    public boolean canHandle(final ProtocolMessage msg) {
        return MessageType.CONNECTION_REQUEST == msg.getType() || MessageType.NODE_STATUS_CHANGE == msg.getType();
    }

    private boolean isMutableRequest(final String method) {
        return "DELETE".equalsIgnoreCase(method) || "POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method);
    }


    /**
     * Callback that is called after an HTTP Request has been replicated to nodes in the cluster.
     * This allows us to disconnect nodes that did not complete the request, if applicable.
     */
    @Override
    public void afterRequest(final String uriPath, final String method, final Set<NodeResponse> nodeResponses) {
        final boolean mutableRequest = isMutableRequest(method);

        /*
         * Nodes that encountered issues handling the request are marked as
         * disconnected for mutable requests (e.g., post, put, delete). For
         * other requests (e.g., get, head), the nodes remain in their current
         * state even if they had problems handling the request.
         */
        if (mutableRequest) {
            final HttpResponseMerger responseMerger = new StandardHttpResponseMerger();
            final Set<NodeResponse> problematicNodeResponses = responseMerger.getProblematicNodeResponses(nodeResponses);

            // all nodes failed
            final boolean allNodesFailed = problematicNodeResponses.size() == nodeResponses.size();

            // some nodes had a problematic response because of a missing counter, ensure the are not disconnected
            final boolean someNodesFailedMissingCounter = !problematicNodeResponses.isEmpty()
                && problematicNodeResponses.size() < nodeResponses.size() && isMissingCounter(problematicNodeResponses, uriPath);

            // ensure nodes stay connected in certain scenarios
            if (allNodesFailed) {
                logger.warn("All nodes failed to process URI {} {}. As a result, no node will be disconnected from cluster", method, uriPath);
                return;
            }

            if (someNodesFailedMissingCounter) {
                return;
            }

            // disconnect problematic nodes
            if (!problematicNodeResponses.isEmpty() && problematicNodeResponses.size() < nodeResponses.size()) {
                logger.warn(String.format("The following nodes failed to process URI %s '%s'.  Requesting each node to disconnect from cluster: ", uriPath, problematicNodeResponses));
                for (final NodeResponse nodeResponse : problematicNodeResponses) {
                    requestNodeDisconnect(nodeResponse.getNodeId(), DisconnectionCode.FAILED_TO_SERVICE_REQUEST, "Failed to process URI to " + method + " " + uriPath);
                }
            }
        }
    }

    /**
     * Determines if all problematic responses were due to 404 NOT_FOUND. Assumes that problematicNodeResponses is not empty and is not comprised of responses from all nodes in the cluster (at least
     * one node contained the counter in question).
     *
     * @param problematicNodeResponses The problematic node responses
     * @param uriPath The path of the URI for the request
     * @return Whether all problematic node responses were due to a missing counter
     */
    private boolean isMissingCounter(final Set<NodeResponse> problematicNodeResponses, final String uriPath) {
        if (COUNTER_URI_PATTERN.matcher(uriPath).matches()) {
            boolean notFound = true;
            for (final NodeResponse problematicResponse : problematicNodeResponses) {
                if (problematicResponse.getStatus() != 404) {
                    notFound = false;
                    break;
                }
            }
            return notFound;
        }
        return false;
    }

    @Override
    public void setConnected(final boolean connected) {
        this.connected = connected;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }
}
