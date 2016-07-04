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

package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.syslog.SyslogAttributes;
import org.apache.nifi.processors.standard.syslog.SyslogEvent;
import org.apache.nifi.processors.standard.syslog.SyslogParser;
import org.apache.nifi.stream.io.StreamUtils;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"logs", "syslog", "attributes", "system", "event", "message"})
@CapabilityDescription("Parses the contents of a Syslog message and adds attributes to the FlowFile for each of the parts of the Syslog message")
@WritesAttributes({@WritesAttribute(attribute = "syslog.priority", description = "The priority of the Syslog message."),
    @WritesAttribute(attribute = "syslog.severity", description = "The severity of the Syslog message derived from the priority."),
    @WritesAttribute(attribute = "syslog.facility", description = "The facility of the Syslog message derived from the priority."),
    @WritesAttribute(attribute = "syslog.version", description = "The optional version from the Syslog message."),
    @WritesAttribute(attribute = "syslog.timestamp", description = "The timestamp of the Syslog message."),
    @WritesAttribute(attribute = "syslog.hostname", description = "The hostname of the Syslog message."),
    @WritesAttribute(attribute = "syslog.sender", description = "The hostname of the Syslog server that sent the message."),
    @WritesAttribute(attribute = "syslog.body", description = "The body of the Syslog message, everything after the hostname.")})
@SeeAlso({ListenSyslog.class, PutSyslog.class})
public class ParseSyslog extends AbstractProcessor {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("Specifies which character set of the Syslog messages")
        .required(true)
        .defaultValue("UTF-8")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that could not be parsed as a Syslog message will be transferred to this Relationship without any attributes being added")
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully parsed as a Syslog message will be to this Relationship.")
        .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(1);
        properties.add(CHARSET);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String charsetName = context.getProperty(CHARSET).getValue();
        final SyslogParser parser = new SyslogParser(Charset.forName(charsetName));
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        final SyslogEvent event;
        try {
            event = parser.parseEvent(buffer, null);
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as a Syslog message due to {}; routing to failure", new Object[] {flowFile, pe});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (!event.isValid()) {
            getLogger().error("Failed to parse {} as a Syslog message: it does not conform to any of the RFC formats supported; routing to failure", new Object[] {flowFile});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = new HashMap<>(8);
        attributes.put(SyslogAttributes.PRIORITY.key(), event.getPriority());
        attributes.put(SyslogAttributes.SEVERITY.key(), event.getSeverity());
        attributes.put(SyslogAttributes.FACILITY.key(), event.getFacility());
        attributes.put(SyslogAttributes.VERSION.key(), event.getVersion());
        attributes.put(SyslogAttributes.TIMESTAMP.key(), event.getTimeStamp());
        attributes.put(SyslogAttributes.HOSTNAME.key(), event.getHostName());
        attributes.put(SyslogAttributes.BODY.key(), event.getMsgBody());

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }

}
