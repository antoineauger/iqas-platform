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
package org.apache.nifi.remote.client.socket;

import org.apache.nifi.remote.Communicant;
import org.apache.nifi.remote.RemoteDestination;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.AbstractSiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SocketClient extends AbstractSiteToSiteClient {

    private static final Logger logger = LoggerFactory.getLogger(SocketClient.class);

    private final EndpointConnectionPool pool;
    private final boolean compress;
    private final String portName;
    private final long penalizationNanos;
    private volatile String portIdentifier;
    private volatile boolean closed = false;

    public SocketClient(final SiteToSiteClientConfig config) {
        super(config);

        final int commsTimeout = (int) config.getTimeout(TimeUnit.MILLISECONDS);
        pool = new EndpointConnectionPool(clusterUrl,
                createRemoteDestination(config.getPortIdentifier(), config.getPortName()),
                commsTimeout,
                (int) config.getIdleConnectionExpiration(TimeUnit.MILLISECONDS),
                config.getSslContext(), config.getEventReporter(), config.getPeerPersistenceFile(),
                siteInfoProvider
        );

        this.compress = config.isUseCompression();
        this.portIdentifier = config.getPortIdentifier();
        this.portName = config.getPortName();
        this.penalizationNanos = config.getPenalizationPeriod(TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean isSecure() throws IOException {
        return siteInfoProvider.isSecure();
    }

    private String getPortIdentifier(final TransferDirection direction) throws IOException {
        final String id = this.portIdentifier;
        if (id != null) {
            return id;
        }

        final String portId;
        if (direction == TransferDirection.SEND) {
            portId = siteInfoProvider.getInputPortIdentifier(this.portName);
        } else {
            portId = siteInfoProvider.getOutputPortIdentifier(this.portName);
        }

        if (portId == null) {
            logger.debug("Unable to resolve port [{}] to an identifier", portName);
        } else {
            logger.debug("Resolved port [{}] to identifier [{}]", portName, portId);
            this.portIdentifier = portId;
        }

        return portId;
    }

    private RemoteDestination createRemoteDestination(final String portId, final String portName) {
        return new RemoteDestination() {
            @Override
            public String getIdentifier() {
                return portId;
            }

            @Override
            public String getName() {
                return portName;
            }

            @Override
            public long getYieldPeriod(final TimeUnit timeUnit) {
                return timeUnit.convert(penalizationNanos, TimeUnit.NANOSECONDS);
            }

            @Override
            public boolean isUseCompression() {
                return compress;
            }
        };
    }

    @Override
    public Transaction createTransaction(final TransferDirection direction) throws IOException {
        if (closed) {
            throw new IllegalStateException("Client is closed");
        }
        final String portId = getPortIdentifier(direction);

        if (portId == null) {
            throw new IOException("Could not find Port with name '" + portName + "' for remote NiFi instance");
        }

        final EndpointConnection connectionState = pool.getEndpointConnection(direction, getConfig());
        if (connectionState == null) {
            return null;
        }

        final Transaction transaction;
        try {
            transaction = connectionState.getSocketClientProtocol().startTransaction(
                    connectionState.getPeer(), connectionState.getCodec(), direction);
        } catch (final Throwable t) {
            pool.terminate(connectionState);
            throw new IOException("Unable to create Transaction to communicate with " + connectionState.getPeer(), t);
        }

        // Wrap the transaction in a new one that will return the EndpointConnectionState back to the pool whenever
        // the transaction is either completed or canceled.
        final AtomicReference<EndpointConnection> connectionStateRef = new AtomicReference<>(connectionState);
        return new Transaction() {
            @Override
            public void confirm() throws IOException {
                transaction.confirm();
            }

            @Override
            public TransactionCompletion complete() throws IOException {
                try {
                    return transaction.complete();
                } finally {
                    final EndpointConnection state = connectionStateRef.get();
                    if (state != null) {
                        pool.offer(connectionState);
                        connectionStateRef.set(null);
                    }
                }
            }

            @Override
            public void cancel(final String explanation) throws IOException {
                try {
                    transaction.cancel(explanation);
                } finally {
                    final EndpointConnection state = connectionStateRef.get();
                    if (state != null) {
                        pool.terminate(connectionState);
                        connectionStateRef.set(null);
                    }
                }
            }

            @Override
            public void error() {
                try {
                    transaction.error();
                } finally {
                    final EndpointConnection state = connectionStateRef.get();
                    if (state != null) {
                        pool.terminate(connectionState);
                        connectionStateRef.set(null);
                    }
                }
            }

            @Override
            public void send(final DataPacket dataPacket) throws IOException {
                transaction.send(dataPacket);
            }

            @Override
            public void send(final byte[] content, final Map<String, String> attributes) throws IOException {
                transaction.send(content, attributes);
            }

            @Override
            public DataPacket receive() throws IOException {
                return transaction.receive();
            }

            @Override
            public TransactionState getState() throws IOException {
                return transaction.getState();
            }

            @Override
            public Communicant getCommunicant() {
                return transaction.getCommunicant();
            }
        };
    }

    @Override
    public void close() throws IOException {
        closed = true;
        pool.shutdown();
    }

}
