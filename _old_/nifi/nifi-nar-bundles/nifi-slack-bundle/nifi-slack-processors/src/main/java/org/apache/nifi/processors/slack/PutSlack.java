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
package org.apache.nifi.processors.slack;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.DataOutputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"put", "slack", "notify"})
@CapabilityDescription("Sends a message to your team on slack.com")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutSlack extends AbstractProcessor {

    public static final PropertyDescriptor WEBHOOK_URL = new PropertyDescriptor
            .Builder()
            .name("webhook-url")
            .displayName("Webhook URL")
            .description("The POST URL provided by Slack to send messages into a channel.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor WEBHOOK_TEXT = new PropertyDescriptor
            .Builder()
            .name("webhook-text")
            .displayName("Webhook Text")
            .description("The text sent in the webhook message")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor
            .Builder()
            .name("channel")
            .displayName("Channel")
            .description("A public channel using #channel or direct message using @username. If not specified, " +
                    "the default webhook channel as specified in Slack's Incoming Webhooks web interface is used.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder()
            .name("username")
            .displayName("Username")
            .description("The displayed Slack username")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ICON_URL = new PropertyDescriptor
            .Builder()
            .name("icon-url")
            .displayName("Icon URL")
            .description("Icon URL to be used for the message")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ICON_EMOJI = new PropertyDescriptor
            .Builder()
            .name("icon-emoji")
            .displayName("Icon Emoji")
            .description("Icon Emoji to be used for the message. Must begin and end with a colon, e.g. :ghost:")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(new EmojiValidator())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to Slack")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to Slack")
            .build();

    public static final List<PropertyDescriptor> descriptors = Collections.unmodifiableList(
            Arrays.asList(WEBHOOK_URL, WEBHOOK_TEXT, CHANNEL, USERNAME, ICON_URL, ICON_EMOJI));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    // Validate the channel (or username for a direct message)
    private String validateChannel(String channel) {
        if ((channel.startsWith("#") || channel.startsWith("@")) && channel.length() > 1) {
            return null;
        }
        return "Channel must begin with '#' or '@'";
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        JsonObjectBuilder builder = Json.createObjectBuilder();
        String text = context.getProperty(WEBHOOK_TEXT).evaluateAttributeExpressions(flowFile).getValue();
        if (text != null && !text.isEmpty()) {
            builder.add("text", text);
        } else {
            // Slack requires the 'text' attribute
            getLogger().error("FlowFile should have non-empty " + WEBHOOK_TEXT.getName());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        String channel = context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue();
        if (channel != null && !channel.isEmpty()) {
            String error = validateChannel(channel);
            if (error == null) {
                builder.add("channel", channel);
            } else {
                getLogger().error("Invalid channel '{}': {}", new Object[]{channel, error});
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        String username = context.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
        if (username != null && !username.isEmpty()) {
            builder.add("username", username);
        }

        String iconUrl = context.getProperty(ICON_URL).evaluateAttributeExpressions(flowFile).getValue();
        if (iconUrl != null && !iconUrl.isEmpty()) {
            builder.add("icon_url", iconUrl);
        }

        String iconEmoji = context.getProperty(ICON_EMOJI).evaluateAttributeExpressions(flowFile).getValue();
        if (iconEmoji != null && !iconEmoji.isEmpty()) {
            builder.add("icon_emoji", iconEmoji);
        }

        JsonObject jsonObject = builder.build();
        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = Json.createWriter(stringWriter);
        jsonWriter.writeObject(jsonObject);
        jsonWriter.close();

        try {
            URL url = new URL(context.getProperty(WEBHOOK_URL).getValue());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            DataOutputStream outputStream  = new DataOutputStream(conn.getOutputStream());
            String payload = "payload=" + URLEncoder.encode(stringWriter.getBuffer().toString(), "UTF-8");
            outputStream.writeBytes(payload);
            outputStream.close();

            int responseCode = conn.getResponseCode();
            if (responseCode >= 200 && responseCode < 300) {
                getLogger().info("Successfully posted message to Slack");
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().send(flowFile, context.getProperty(WEBHOOK_URL).getValue());
            } else {
                getLogger().error("Failed to post message to Slack with response code {}", new Object[]{responseCode});
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            }
        } catch (IOException e) {
            getLogger().error("Failed to open connection", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private static class EmojiValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (input.startsWith(":") && input.endsWith(":") && input.length() > 2) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

            return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                    .explanation("Must begin and end with a colon")
                    .build();
        }
    }
}
