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
package org.apache.nifi.amqp.processors;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

public class ConsumeAMQPTest {


    @Test
    public void validateSuccessfullConsumeAndTransferToSuccess() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1", "queue2"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, "myExchange", "key1", null)) {
            sender.publish("hello".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN);

            ConsumeAMQP pubProc = new LocalConsumeAMQP(connection);
            TestRunner runner = TestRunners.newTestRunner(pubProc);
            runner.setProperty(ConsumeAMQP.HOST, "injvm");
            runner.setProperty(ConsumeAMQP.QUEUE, "queue1");

            runner.run();
            Thread.sleep(200);
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
        }

    }

    public static class LocalConsumeAMQP extends ConsumeAMQP {

        private final Connection conection;
        public LocalConsumeAMQP(Connection connection) {
            this.conection = connection;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            synchronized (this) {
                if (this.amqpConnection == null || !this.amqpConnection.isOpen()) {
                    this.amqpConnection = this.conection;
                    this.targetResource = this.finishBuildingTargetResource(context);
                }
            }
            this.rendezvousWithAmqp(context, session);
        }

        public Connection getConnection() {
            return this.amqpConnection;
        }
    }
}
