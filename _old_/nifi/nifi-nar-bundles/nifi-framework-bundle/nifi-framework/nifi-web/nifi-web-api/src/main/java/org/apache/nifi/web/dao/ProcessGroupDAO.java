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
package org.apache.nifi.web.dao;

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;

import java.util.Set;

public interface ProcessGroupDAO {

    /**
     * Determines if the specified remote process group exists.
     *
     * @return true if group exists
     */
    boolean hasProcessGroup(String groupId);

    /**
     * Creates a process group reference.
     *
     * @param parentGroupId The parent process group id
     * @param processGroup The process group
     * @return The process group
     */
    ProcessGroup createProcessGroup(String parentGroupId, ProcessGroupDTO processGroup);

    /**
     * Gets the specified process group.
     *
     * @param groupId The process group id
     * @return The process group
     */
    ProcessGroup getProcessGroup(String groupId);

    /**
     * Gets all of the process groups.
     *
     * @param parentGroupId The parent group id
     * @return The process groups
     */
    Set<ProcessGroup> getProcessGroups(String parentGroupId);

    /**
     * Verifies the specified process group can be modified.
     *
     * @param groupId id
     * @param state scheduled state
     */
    void verifyScheduleComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Schedules the components in the specified process group.
     *
     * @param groupId id
     * @param state scheduled state
     */
    void scheduleComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Updates the specified process group.
     *
     * @param processGroup The process group
     * @return The process group
     */
    ProcessGroup updateProcessGroup(ProcessGroupDTO processGroup);

    /**
     * Verifies the specified process group can be removed.
     *
     * @param groupId id
     */
    void verifyDelete(String groupId);

    /**
     * Deletes the specified process group.
     *
     * @param groupId The process group id
     */
    void deleteProcessGroup(String groupId);
}
