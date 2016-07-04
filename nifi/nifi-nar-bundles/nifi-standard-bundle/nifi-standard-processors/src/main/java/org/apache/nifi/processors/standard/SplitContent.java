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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.NaiveSearchRingBuffer;
import org.apache.nifi.util.Tuple;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"content", "split", "binary"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits incoming FlowFiles by a specified byte sequence")
@WritesAttributes({
    @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")})
@SeeAlso(MergeContent.class)
public class SplitContent extends AbstractProcessor {

    // attribute keys
    public static final String FRAGMENT_ID = "fragment.identifier";
    public static final String FRAGMENT_INDEX = "fragment.index";
    public static final String FRAGMENT_COUNT = "fragment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME = "segment.original.filename";

    static final AllowableValue HEX_FORMAT = new AllowableValue("Hexadecimal", "Hexadecimal", "The Byte Sequence will be interpreted as a hexadecimal representation of bytes");
    static final AllowableValue UTF8_FORMAT = new AllowableValue("Text", "Text", "The Byte Sequence will be interpreted as UTF-8 Encoded text");

    static final AllowableValue TRAILING_POSITION = new AllowableValue("Trailing", "Trailing", "Keep the Byte Sequence at the end of the first split if <Keep Byte Sequence> is true");
    static final AllowableValue LEADING_POSITION = new AllowableValue("Leading", "Leading", "Keep the Byte Sequence at the beginning of the second split if <Keep Byte Sequence> is true");

    public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
            .name("Byte Sequence Format")
            .description("Specifies how the <Byte Sequence> property should be interpreted")
            .required(true)
            .allowableValues(HEX_FORMAT, UTF8_FORMAT)
            .defaultValue(HEX_FORMAT.getValue())
            .build();
    public static final PropertyDescriptor BYTE_SEQUENCE = new PropertyDescriptor.Builder()
            .name("Byte Sequence")
            .description("A representation of bytes to look for and upon which to split the source file into separate files")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor KEEP_SEQUENCE = new PropertyDescriptor.Builder()
            .name("Keep Byte Sequence")
            .description("Determines whether or not the Byte Sequence should be included with each Split")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor BYTE_SEQUENCE_LOCATION = new PropertyDescriptor.Builder()
            .name("Byte Sequence Location")
            .description("If <Keep Byte Sequence> is set to true, specifies whether the byte sequence should be added to the end of the first "
                    + "split or the beginning of the second; if <Keep Byte Sequence> is false, this property is ignored.")
            .required(true)
            .allowableValues(TRAILING_POSITION, LEADING_POSITION)
            .defaultValue(TRAILING_POSITION.getValue())
            .build();

    public static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("All Splits will be routed to the splits relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original file")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<byte[]> byteSequence = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SPLITS);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FORMAT);
        properties.add(BYTE_SEQUENCE);
        properties.add(KEEP_SEQUENCE);
        properties.add(BYTE_SEQUENCE_LOCATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        final String format = validationContext.getProperty(FORMAT).getValue();
        if (HEX_FORMAT.getValue().equals(format)) {
            final String byteSequence = validationContext.getProperty(BYTE_SEQUENCE).getValue();
            final ValidationResult result = new HexStringPropertyValidator().validate(BYTE_SEQUENCE.getName(), byteSequence, validationContext);
            results.add(result);
        }
        return results;
    }

    @OnScheduled
    public void initializeByteSequence(final ProcessContext context) throws DecoderException {
        final String bytePattern = context.getProperty(BYTE_SEQUENCE).getValue();

        final String format = context.getProperty(FORMAT).getValue();
        if (HEX_FORMAT.getValue().equals(format)) {
            this.byteSequence.set(Hex.decodeHex(bytePattern.toCharArray()));
        } else {
            this.byteSequence.set(bytePattern.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final boolean keepSequence = context.getProperty(KEEP_SEQUENCE).asBoolean();
        final boolean keepTrailingSequence;
        final boolean keepLeadingSequence;
        if (keepSequence) {
            if (context.getProperty(BYTE_SEQUENCE_LOCATION).getValue().equals(TRAILING_POSITION.getValue())) {
                keepTrailingSequence = true;
                keepLeadingSequence = false;
            } else {
                keepTrailingSequence = false;
                keepLeadingSequence = true;
            }
        } else {
            keepTrailingSequence = false;
            keepLeadingSequence = false;
        }

        final byte[] byteSequence = this.byteSequence.get();
        if (byteSequence == null) {   // should never happen. But just in case...
            logger.error("{} Unable to obtain Byte Sequence", new Object[]{this});
            session.rollback();
            return;
        }

        final List<Tuple<Long, Long>> splits = new ArrayList<>();

        final NaiveSearchRingBuffer buffer = new NaiveSearchRingBuffer(byteSequence);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                long bytesRead = 0L;
                long startOffset = 0L;

                try (final InputStream in = new BufferedInputStream(rawIn)) {
                    while (true) {
                        final int nextByte = in.read();
                        if (nextByte == -1) {
                            return;
                        }

                        bytesRead++;
                        boolean matched = buffer.addAndCompare((byte) (nextByte & 0xFF));
                        if (matched) {
                            long splitLength;

                            if (keepTrailingSequence) {
                                splitLength = bytesRead - startOffset;
                            } else {
                                splitLength = bytesRead - startOffset - byteSequence.length;
                            }

                            if (keepLeadingSequence && startOffset > 0) {
                                splitLength += byteSequence.length;
                            }

                            final long splitStart = (keepLeadingSequence && startOffset > 0) ? startOffset - byteSequence.length : startOffset;
                            splits.add(new Tuple<>(splitStart, splitLength));
                            startOffset = bytesRead;
                            buffer.clear();
                        }
                    }
                }
            }
        });

        long lastOffsetPlusSize = -1L;
        if (splits.isEmpty()) {
            FlowFile clone = session.clone(flowFile);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(clone, REL_SPLITS);
            logger.info("Found no match for {}; transferring original 'original' and transferring clone {} to 'splits'", new Object[]{flowFile, clone});
            return;
        }

        final ArrayList<FlowFile> splitList = new ArrayList<>();
        for (final Tuple<Long, Long> tuple : splits) {
            long offset = tuple.getKey();
            long size = tuple.getValue();
            if (size > 0) {
                FlowFile split = session.clone(flowFile, offset, size);
                splitList.add(split);
            }

            lastOffsetPlusSize = offset + size;
        }

        // lastOffsetPlusSize indicates the ending position of the last split.
        // if the data didn't end with the byte sequence, we need one final split to run from the end
        // of the last split to the end of the content.
        long finalSplitOffset = lastOffsetPlusSize;
        if (!keepTrailingSequence && !keepLeadingSequence) {
            finalSplitOffset += byteSequence.length;
        }
        if (finalSplitOffset > -1L && finalSplitOffset < flowFile.getSize()) {
            FlowFile finalSplit = session.clone(flowFile, finalSplitOffset, flowFile.getSize() - finalSplitOffset);
            splitList.add(finalSplit);
        }

        finishFragmentAttributes(session, flowFile, splitList);
        session.transfer(splitList, REL_SPLITS);
        session.transfer(flowFile, REL_ORIGINAL);

        if (splitList.size() > 10) {
            logger.info("Split {} into {} files", new Object[]{flowFile, splitList.size()});
        } else {
            logger.info("Split {} into {} files: {}", new Object[]{flowFile, splitList.size(), splitList});
        }
    }

    /**
     * Apply split index, count and other attributes.
     *
     * @param session session
     * @param source source
     * @param splits splits
     */
    private void finishFragmentAttributes(final ProcessSession session, final FlowFile source, final List<FlowFile> splits) {
        final String originalFilename = source.getAttribute(CoreAttributes.FILENAME.key());

        final String fragmentId = UUID.randomUUID().toString();
        final ArrayList<FlowFile> newList = new ArrayList<>(splits);
        splits.clear();
        for (int i = 1; i <= newList.size(); i++) {
            FlowFile ff = newList.get(i - 1);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(FRAGMENT_ID, fragmentId);
            attributes.put(FRAGMENT_INDEX, String.valueOf(i));
            attributes.put(FRAGMENT_COUNT, String.valueOf(newList.size()));
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFilename);
            FlowFile newFF = session.putAllAttributes(ff, attributes);
            splits.add(newFF);
        }
    }

    static class HexStringPropertyValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                Hex.decodeHex(input.toCharArray());
                return new ValidationResult.Builder().valid(true).input(input).subject(subject).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().valid(false).explanation("Not a valid Hex String").input(input).subject(subject).build();
            }
        }
    }
}
