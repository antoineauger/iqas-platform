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

package org.apache.nifi.controller.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateProviderInitializationContext;

public class StandardStateProviderInitializationContext implements StateProviderInitializationContext {
    private final String id;
    private final Map<PropertyDescriptor, PropertyValue> properties;
    private final SSLContext sslContext;

    public StandardStateProviderInitializationContext(final String identifier, final Map<PropertyDescriptor, PropertyValue> properties, final SSLContext sslContext) {
        this.id = identifier;
        this.properties = new HashMap<>(properties);
        this.sslContext = sslContext;
    }

    @Override
    public Map<PropertyDescriptor, PropertyValue> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        return properties.get(property);
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public SSLContext getSSLContext() {
        return sslContext;
    }
}
