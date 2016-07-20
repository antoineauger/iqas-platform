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
package org.apache.nifi.web.security;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base AuthenticationProvider that provides common functionality to mapping identities.
 */
public abstract class NiFiAuthenticationProvider implements AuthenticationProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(NiFiAuthenticationProvider.class);
    private static final Pattern backReferencePattern = Pattern.compile("\\$(\\d+)");

    private NiFiProperties properties;
    private List<IdentityMapping> mappings;

    /**
     * @param properties the NiFiProperties instance
     */
    public NiFiAuthenticationProvider(final NiFiProperties properties) {
        this.properties = properties;
        this.mappings = Collections.unmodifiableList(getIdentityMappings(properties));
    }

    /**
     * Builds the identity mappings from NiFiProperties.
     *
     * @param properties the NiFiProperties instance
     * @return a list of identity mappings
     */
    private List<IdentityMapping> getIdentityMappings(final NiFiProperties properties) {
        final List<IdentityMapping> mappings = new ArrayList<>();

        // go through each property
        for (String propertyName : properties.stringPropertyNames()) {
            if (StringUtils.startsWith(propertyName, NiFiProperties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX)) {
                final String key = StringUtils.substringAfter(propertyName, NiFiProperties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX);
                final String identityPattern = properties.getProperty(propertyName);

                if (StringUtils.isBlank(identityPattern)) {
                    LOGGER.warn("Identity Mapping property {} was found, but was empty", new Object[]{propertyName});
                    continue;
                }

                final String identityValueProperty = NiFiProperties.SECURITY_IDENTITY_MAPPING_VALUE_PREFIX + key;
                final String identityValue = properties.getProperty(identityValueProperty);

                if (StringUtils.isBlank(identityValue)) {
                    LOGGER.warn("Identity Mapping property {} was found, but corresponding value {} was not found",
                            new Object[]{propertyName, identityValueProperty});
                    continue;
                }

                final IdentityMapping identityMapping = new IdentityMapping(key, Pattern.compile(identityPattern), identityValue);
                mappings.add(identityMapping);

                LOGGER.debug("Found Identity Mapping with key = {}, pattern = {}, value = {}",
                        new Object[] {key, identityPattern, identityValue});
            }
        }

        // sort the list by the key so users can control the ordering in nifi.properties
        Collections.sort(mappings, new Comparator<IdentityMapping>() {
            @Override
            public int compare(IdentityMapping m1, IdentityMapping m2) {
                return m1.getKey().compareTo(m2.getKey());
            }
        });

        return mappings;
    }

    public List<IdentityMapping> getMappings() {
        return mappings;
    }

    protected String mapIdentity(final String identity) {
        for (IdentityMapping mapping : mappings) {
            Matcher m = mapping.getPattern().matcher(identity);
            if (m.matches()) {
                final String pattern = mapping.getPattern().pattern();
                final String replacementValue = escapeLiteralBackReferences(mapping.getReplacementValue(), m.groupCount());
                return identity.replaceAll(pattern, replacementValue);
            }
        }

        return identity;
    }

    // If we find a back reference that is not valid, then we will treat it as a literal string. For example, if we have 3 capturing
    // groups and the Replacement Value has the value is "I owe $8 to him", then we want to treat the $8 as a literal "$8", rather
    // than attempting to use it as a back reference.
    private static String escapeLiteralBackReferences(final String unescaped, final int numCapturingGroups) {
        if (numCapturingGroups == 0) {
            return unescaped;
        }

        String value = unescaped;
        final Matcher backRefMatcher = backReferencePattern.matcher(value);
        while (backRefMatcher.find()) {
            final String backRefNum = backRefMatcher.group(1);
            if (backRefNum.startsWith("0")) {
                continue;
            }
            final int originalBackRefIndex = Integer.parseInt(backRefNum);
            int backRefIndex = originalBackRefIndex;

            // if we have a replacement value like $123, and we have less than 123 capturing groups, then
            // we want to truncate the 3 and use capturing group 12; if we have less than 12 capturing groups,
            // then we want to truncate the 2 and use capturing group 1; if we don't have a capturing group then
            // we want to truncate the 1 and get 0.
            while (backRefIndex > numCapturingGroups && backRefIndex >= 10) {
                backRefIndex /= 10;
            }

            if (backRefIndex > numCapturingGroups) {
                final StringBuilder sb = new StringBuilder(value.length() + 1);
                final int groupStart = backRefMatcher.start(1);

                sb.append(value.substring(0, groupStart - 1));
                sb.append("\\");
                sb.append(value.substring(groupStart - 1));
                value = sb.toString();
            }
        }

        return value;
    }

    /**
     * Holder to pass around the key, pattern, and replacement from an identity mapping in NiFiProperties.
     */
    public static final class IdentityMapping {

        private final String key;
        private final Pattern pattern;
        private final String replacementValue;

        public IdentityMapping(String key, Pattern pattern, String replacementValue) {
            this.key = key;
            this.pattern = pattern;
            this.replacementValue = replacementValue;
        }

        public String getKey() {
            return key;
        }

        public Pattern getPattern() {
            return pattern;
        }

        public String getReplacementValue() {
            return replacementValue;
        }

    }

}
