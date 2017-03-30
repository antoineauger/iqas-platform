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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class TestSiteToSiteClient {

    @Test
    @Ignore("For local testing only; not really a unit test but a manual test")
    public void testReceive() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");

        final SiteToSiteClient client = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("cba")
                .requestBatchCount(10)
                .build();

        try {
            for (int i = 0; i < 1000; i++) {
                final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
                Assert.assertNotNull(transaction);

                DataPacket packet;
                while (true) {
                    packet = transaction.receive();
                    if (packet == null) {
                        break;
                    }

                    final InputStream in = packet.getData();
                    final long size = packet.getSize();
                    final byte[] buff = new byte[(int) size];

                    StreamUtils.fillBuffer(in, buff);
                }

                transaction.confirm();
                transaction.complete();
            }
        } finally {
            client.close();
        }
    }

    @Test
    @Ignore("For local testing only; not really a unit test but a manual test")
    public void testSend() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");

        final SiteToSiteClient client = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("input")
                .build();

        try {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);
            Assert.assertNotNull(transaction);

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("site-to-site", "yes, please!");
            final byte[] bytes = "Hello".getBytes();
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            final DataPacket packet = new StandardDataPacket(attrs, bais, bytes.length);
            transaction.send(packet);

            transaction.confirm();
            transaction.complete();
        } finally {
            client.close();
        }
    }

    @Test
    public void testSerialization() {
        final SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("input")
                .buildConfig();

        final Kryo kryo = new Kryo();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Output output = new Output(out);

        try {
            kryo.writeObject(output, clientConfig);
        } finally {
            output.close();
        }

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final Input input = new Input(in);

        try {
            SiteToSiteClientConfig clientConfig2 = kryo.readObject(input, SiteToSiteClient.StandardSiteToSiteClientConfig.class);
            Assert.assertEquals(clientConfig.getUrl(), clientConfig2.getUrl());
        } finally {
            input.close();
        }
    }

}
