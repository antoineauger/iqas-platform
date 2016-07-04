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
package org.apache.nifi.jms.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.nifi.jms.processors.JMSConsumer.JMSResponse;
import org.apache.nifi.logging.ComponentLog;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.support.JmsHeaders;

public class JMSPublisherConsumerTest {

    @Test
    public void validateByesConvertedToBytesMessageOnSend() throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination("testQueue", false);

        JMSPublisher publisher = new JMSPublisher(jmsTemplate, mock(ComponentLog.class));
        publisher.publish("hellomq".getBytes());

        Message receivedMessage = jmsTemplate.receive();
        assertTrue(receivedMessage instanceof BytesMessage);
        byte[] bytes = new byte[7];
        ((BytesMessage) receivedMessage).readBytes(bytes);
        assertEquals("hellomq", new String(bytes));

        ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
    }

    @Test
    public void validateJmsHeadersAndPropertiesAreTransferredFromFFAttributes() throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination("testQueue", false);

        JMSPublisher publisher = new JMSPublisher(jmsTemplate, mock(ComponentLog.class));
        Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("foo", "foo");
        flowFileAttributes.put(JmsHeaders.REPLY_TO, "myTopic");
        publisher.publish("hellomq".getBytes(), flowFileAttributes);

        Message receivedMessage = jmsTemplate.receive();
        assertTrue(receivedMessage instanceof BytesMessage);
        assertEquals("foo", receivedMessage.getStringProperty("foo"));
        assertTrue(receivedMessage.getJMSReplyTo() instanceof Topic);
        assertEquals("myTopic", ((Topic) receivedMessage.getJMSReplyTo()).getTopicName());

        ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
    }

    /**
     * At the moment the only two supported message types are TextMessage and
     * BytesMessage which is sufficient for the type if JMS use cases NiFi is
     * used. The may change to the point where all message types are supported
     * at which point this test will no be longer required.
     */
    @Test(expected = IllegalStateException.class)
    public void validateFailOnUnsupportedMessageType() throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination("testQueue", false);

        jmsTemplate.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createObjectMessage();
            }
        });

        JMSConsumer consumer = new JMSConsumer(jmsTemplate, mock(ComponentLog.class));
        try {
            consumer.consume();
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void validateConsumeWithCustomHeadersAndProperties() throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination("testQueue", false);

        jmsTemplate.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                TextMessage message = session.createTextMessage("hello from the other side");
                message.setStringProperty("foo", "foo");
                message.setBooleanProperty("bar", false);
                message.setJMSReplyTo(session.createQueue("fooQueue"));
                return message;
            }
        });

        JMSConsumer consumer = new JMSConsumer(jmsTemplate, mock(ComponentLog.class));
        assertEquals("JMSConsumer[destination:testQueue; pub-sub:false;]", consumer.toString());

        JMSResponse response = consumer.consume();
        assertEquals("hello from the other side", new String(response.getMessageBody()));
        assertEquals("fooQueue", response.getMessageHeaders().get(JmsHeaders.REPLY_TO));
        assertEquals("foo", response.getMessageProperties().get("foo"));
        assertEquals("false", response.getMessageProperties().get("bar"));

        ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
    }
}
