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
package org.apache.nifi.websocket;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.websocket.jetty.JettyWebSocketClient;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;


public class TestJettyWebSocketClient {

    @Test
    public void testValidationRequiredProperties() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertEquals(JettyWebSocketClient.WS_URI.getDisplayName(), result.getSubject());
    }

    @Test
    public void testValidationSuccess() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        context.setCustomValue(JettyWebSocketClient.WS_URI, "ws://localhost:9001/test");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(0, results.size());
    }

    @Test
    public void testValidationProtocol() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        context.setCustomValue(JettyWebSocketClient.WS_URI, "http://localhost:9001/test");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertEquals(JettyWebSocketClient.WS_URI.getName(), result.getSubject());
    }

}
