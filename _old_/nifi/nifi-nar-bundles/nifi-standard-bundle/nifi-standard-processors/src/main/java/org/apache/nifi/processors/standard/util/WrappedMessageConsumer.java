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
package org.apache.nifi.processors.standard.util;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.nifi.logging.ComponentLog;

public class WrappedMessageConsumer {

    private final Connection connection;
    private final Session session;
    private final MessageConsumer consumer;

    private boolean closed = false;

    public WrappedMessageConsumer(final Connection connection, final Session jmsSession, final MessageConsumer messageConsumer) {
        this.connection = connection;
        this.session = jmsSession;
        this.consumer = messageConsumer;
    }

    public Connection getConnection() {
        return connection;
    }

    public Session getSession() {
        return session;
    }

    public MessageConsumer getConsumer() {
        return consumer;
    }

    public void close(final ComponentLog logger) {
        closed = true;

        try {
            connection.close();
        } catch (final JMSException e) {
            logger.warn("unable to close connection to JMS Server due to {}; resources may not be cleaned up appropriately", e);
        }

        try {
            session.close();
        } catch (final JMSException e) {
            logger.warn("unable to close connection to JMS Server due to {}; resources may not be cleaned up appropriately", e);
        }

        try {
            consumer.close();
        } catch (final JMSException e) {
            logger.warn("unable to close connection to JMS Server due to {}; resources may not be cleaned up appropriately", e);
        }
    }

    public boolean isClosed() {
        return closed;
    }
}
