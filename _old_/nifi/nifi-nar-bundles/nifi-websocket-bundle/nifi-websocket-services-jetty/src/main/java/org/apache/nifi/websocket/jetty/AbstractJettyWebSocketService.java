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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.websocket.AbstractWebSocketService;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractJettyWebSocketService extends AbstractWebSocketService {

    public static final PropertyDescriptor INPUT_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("input-buffer-size")
            .displayName("Input Buffer Size")
            .description("The size of the input (read from network layer) buffer size.")
            .required(true)
            .defaultValue("4 kb")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_TEXT_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("max-text-message-size")
            .displayName("Max Text Message Size")
            .description("The maximum size of a text message during parsing/generating.")
            .required(true)
            .defaultValue("64 kb")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BINARY_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("max-binary-message-size")
            .displayName("Max Binary Message Size")
            .description("The maximum size of a binary message during parsing/generating.")
            .required(true)
            .defaultValue("64 kb")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    static List<PropertyDescriptor> getAbstractPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INPUT_BUFFER_SIZE);
        descriptors.add(MAX_TEXT_MESSAGE_SIZE);
        descriptors.add(MAX_BINARY_MESSAGE_SIZE);
        return descriptors;
    }


    protected SslContextFactory createSslFactory(final SSLContextService sslService, final boolean needClientAuth, final boolean wantClientAuth) {
        final SslContextFactory sslFactory = new SslContextFactory();

        sslFactory.setNeedClientAuth(needClientAuth);
        sslFactory.setWantClientAuth(wantClientAuth);

        if (sslService.isKeyStoreConfigured()) {
            sslFactory.setKeyStorePath(sslService.getKeyStoreFile());
            sslFactory.setKeyStorePassword(sslService.getKeyStorePassword());
            sslFactory.setKeyStoreType(sslService.getKeyStoreType());
        }

        if (sslService.isTrustStoreConfigured()) {
            sslFactory.setTrustStorePath(sslService.getTrustStoreFile());
            sslFactory.setTrustStorePassword(sslService.getTrustStorePassword());
            sslFactory.setTrustStoreType(sslService.getTrustStoreType());
        }

        return sslFactory;
    }

    protected void configurePolicy(final ConfigurationContext context, final WebSocketPolicy policy) {
        final int inputBufferSize = context.getProperty(INPUT_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxTextMessageSize = context.getProperty(MAX_TEXT_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxBinaryMessageSize = context.getProperty(MAX_BINARY_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
        policy.setInputBufferSize(inputBufferSize);
        policy.setMaxTextMessageSize(maxTextMessageSize);
        policy.setMaxBinaryMessageSize(maxBinaryMessageSize);
    }

}
