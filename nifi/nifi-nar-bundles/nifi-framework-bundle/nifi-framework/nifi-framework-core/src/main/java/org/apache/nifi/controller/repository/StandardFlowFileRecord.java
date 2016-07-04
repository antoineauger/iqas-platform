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
package org.apache.nifi.controller.repository;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

/**
 * <p>
 * A flow file is a logical notion of an item in a flow with its associated attributes and identity which can be used as a reference for its actual content.
 * </p>
 *
 * <b>Immutable - Thread Safe</b>
 *
 */
public final class StandardFlowFileRecord implements FlowFile, FlowFileRecord {
    private static final int MAX_LINEAGE_IDENTIFIERS = 100;

    private final long id;
    private final long entryDate;
    private final long lineageStartDate;
    private final Set<String> lineageIdentifiers;
    private final long size;
    private final long penaltyExpirationMs;
    private final Map<String, String> attributes;
    private final ContentClaim claim;
    private final long claimOffset;
    private final long lastQueueDate;

    private StandardFlowFileRecord(final Builder builder) {
        this.id = builder.bId;
        this.attributes = builder.bAttributes;
        this.entryDate = builder.bEntryDate;
        this.lineageStartDate = builder.bLineageStartDate;
        this.lineageIdentifiers = builder.bLineageIdentifiers;
        this.penaltyExpirationMs = builder.bPenaltyExpirationMs;
        this.size = builder.bSize;
        this.claim = builder.bClaim;
        this.claimOffset = builder.bClaimOffset;
        this.lastQueueDate = builder.bLastQueueDate;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public long getEntryDate() {
        return entryDate;
    }

    @Override
    public Set<String> getLineageIdentifiers() {
        return Collections.unmodifiableSet(lineageIdentifiers);
    }

    @Override
    public long getLineageStartDate() {
        return lineageStartDate;
    }

    @Override
    public Long getLastQueueDate() {
        return lastQueueDate;
    }

    @Override
    public boolean isPenalized() {
        return penaltyExpirationMs > 0 ? penaltyExpirationMs > System.currentTimeMillis() : false;
    }

    @Override
    public String getAttribute(final String key) {
        return attributes.get(key);
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(this.attributes);
    }

    @Override
    public ContentClaim getContentClaim() {
        return this.claim;
    }

    @Override
    public long getContentClaimOffset() {
        return this.claimOffset;
    }

    /**
     * Provides the natural ordering for FlowFile objects which is based on their identifier.
     *
     * @param other other
     * @return standard compare contract
     */
    @Override
    public int compareTo(final FlowFile other) {
        return new CompareToBuilder().append(id, other.getId()).toComparison();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StandardFlowFileRecord)) {
            return false;
        }
        final StandardFlowFileRecord otherRecord = (StandardFlowFileRecord) other;
        return new EqualsBuilder().append(id, otherRecord.id).isEquals();
    }

    @Override
    public String toString() {
        final ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("uuid", getAttribute(CoreAttributes.UUID.key()));
        builder.append("claim", claim == null ? "" : claim.toString());
        builder.append("offset", claimOffset);
        builder.append("name", getAttribute(CoreAttributes.FILENAME.key())).append("size", size);
        return builder.toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(7, 13).append(id).toHashCode();
    }

    public static final class Builder {

        private long bId;
        private long bEntryDate = System.currentTimeMillis();
        private long bLineageStartDate = bEntryDate;
        private final Set<String> bLineageIdentifiers = new HashSet<>();
        private long bPenaltyExpirationMs = -1L;
        private long bSize = 0L;
        private final Map<String, String> bAttributes = new HashMap<>();
        private ContentClaim bClaim = null;
        private long bClaimOffset = 0L;
        private long bLastQueueDate = System.currentTimeMillis();

        public Builder id(final long id) {
            bId = id;
            return this;
        }

        public Builder lineageIdentifiers(final Collection<String> lineageIdentifiers) {
            if (null != lineageIdentifiers) {
                bLineageIdentifiers.clear();

                if (lineageIdentifiers.size() > MAX_LINEAGE_IDENTIFIERS) {
                    int i = 0;
                    for (final String id : lineageIdentifiers) {
                        bLineageIdentifiers.add(id);
                        if (i++ >= MAX_LINEAGE_IDENTIFIERS) {
                            break;
                        }
                    }
                } else {
                    bLineageIdentifiers.addAll(lineageIdentifiers);
                }
            }
            return this;
        }

        public Builder entryDate(final long epochMs) {
            bEntryDate = epochMs;
            return this;
        }

        public Builder lineageStartDate(final long lineageStartDate) {
            bLineageStartDate = lineageStartDate;
            return this;
        }

        public Builder penaltyExpirationTime(final long epochMilliseconds) {
            bPenaltyExpirationMs = epochMilliseconds;
            return this;
        }

        public Builder size(final long bytes) {
            if (bytes >= 0) {
                bSize = bytes;
            }
            return this;
        }

        public Builder addAttribute(final String key, final String value) {
            if (key != null && value != null) {
                bAttributes.put(FlowFile.KeyValidator.validateKey(key), value);
            }
            return this;
        }

        public Builder addAttributes(final Map<String, String> attributes) {
            if (null != attributes) {
                for (final String key : attributes.keySet()) {
                    FlowFile.KeyValidator.validateKey(key);
                }
                for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                    final String key = entry.getKey();
                    final String value = entry.getValue();
                    if (key != null && value != null) {
                        bAttributes.put(key, value);
                    }
                }
            }
            return this;
        }

        public Builder removeAttributes(final String... keys) {
            if (keys != null) {
                for (final String key : keys) {
                    if (CoreAttributes.UUID.key().equals(key)) {
                        continue;
                    }

                    bAttributes.remove(key);
                }
            }
            return this;
        }

        public Builder removeAttributes(final Set<String> keys) {
            if (keys != null) {
                for (final String key : keys) {
                    if (CoreAttributes.UUID.key().equals(key)) {
                        continue;
                    }

                    bAttributes.remove(key);
                }
            }
            return this;
        }

        public Builder removeAttributes(final Pattern keyPattern) {
            if (keyPattern != null) {
                final Iterator<String> iterator = bAttributes.keySet().iterator();
                while (iterator.hasNext()) {
                    final String key = iterator.next();

                    if (CoreAttributes.UUID.key().equals(key)) {
                        continue;
                    }

                    if (keyPattern.matcher(key).matches()) {
                        iterator.remove();
                    }
                }
            }
            return this;
        }

        public Builder contentClaim(final ContentClaim claim) {
            this.bClaim = claim;
            return this;
        }

        public Builder contentClaimOffset(final long offset) {
            this.bClaimOffset = offset;
            return this;
        }

        public Builder lastQueueDate(final long lastQueueDate) {
            this.bLastQueueDate = lastQueueDate;
            return this;
        }

        public Builder fromFlowFile(final FlowFileRecord specFlowFile) {
            if (specFlowFile == null) {
                return this;
            }
            bId = specFlowFile.getId();
            bEntryDate = specFlowFile.getEntryDate();
            bLineageStartDate = specFlowFile.getLineageStartDate();
            bLineageIdentifiers.clear();
            bLineageIdentifiers.addAll(specFlowFile.getLineageIdentifiers());
            bPenaltyExpirationMs = specFlowFile.getPenaltyExpirationMillis();
            bSize = specFlowFile.getSize();
            bAttributes.putAll(specFlowFile.getAttributes());
            bClaim = specFlowFile.getContentClaim();
            bClaimOffset = specFlowFile.getContentClaimOffset();

            return this;
        }

        public FlowFileRecord build() {
            return new StandardFlowFileRecord(this);
        }
    }

    @Override
    public long getPenaltyExpirationMillis() {
        return penaltyExpirationMs;
    }
}
