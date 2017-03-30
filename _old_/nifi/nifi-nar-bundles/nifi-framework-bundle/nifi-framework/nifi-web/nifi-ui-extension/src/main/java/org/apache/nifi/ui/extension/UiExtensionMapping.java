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
package org.apache.nifi.ui.extension;

import java.util.List;
import java.util.Map;

/**
 * Mapping of all discovered UI extensions.
 */
public class UiExtensionMapping {

    private final Map<String, List<UiExtension>> uiExtensions;

    public UiExtensionMapping(Map<String, List<UiExtension>> uiExtensions) {
        this.uiExtensions = uiExtensions;
    }

    /**
     * @param type type
     * @return whether there are any UI extensions for the specified component
     * type
     */
    public boolean hasUiExtension(final String type) {
        return uiExtensions.containsKey(type);
    }

    /**
     * @param type type
     * @return the listing of all discovered UI extensions for the specified
     * component type
     */
    public List<UiExtension> getUiExtension(final String type) {
        return uiExtensions.get(type);
    }

}
