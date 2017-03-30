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
package org.apache.nifi.cluster.protocol.impl.testutils;

import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;

/**
 */
public class ReflexiveProtocolHandler implements ProtocolHandler {

    private List<ProtocolMessage> messages = new ArrayList<>();

    @Override
    public ProtocolMessage handle(ProtocolMessage msg) throws ProtocolException {
        messages.add(msg);
        return msg;
    }

    @Override
    public boolean canHandle(ProtocolMessage msg) {
        return true;
    }

    public List<ProtocolMessage> getMessages() {
        return messages;
    }

}
