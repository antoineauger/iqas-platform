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
package org.apache.nifi.provenance;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;

public class AsyncQuerySubmission implements QuerySubmission {

    public static final int TTL = (int) TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

    private final Date submissionTime = new Date();
    private final Query query;

    private volatile boolean canceled = false;
    private final StandardQueryResult queryResult;
    private final String submitterId;

    private final List<Future<?>> queryExecutions = new ArrayList<>();

    /**
     * Constructs an AsyncQuerySubmission with the given query and the given
     * number of steps, indicating how many results must be added to this
     * AsyncQuerySubmission before it is considered finished
     *
     * @param query the query to execute
     * @param numSteps how many steps to include
     */
    public AsyncQuerySubmission(final Query query, final int numSteps, final String submitterId) {
        this.query = query;
        this.submitterId = submitterId;
        queryResult = new StandardQueryResult(query, numSteps);
    }

    @Override
    public String getSubmitterIdentity() {
        return submitterId;
    }

    @Override
    public Date getSubmissionTime() {
        return submissionTime;
    }

    @Override
    public String getQueryIdentifier() {
        return query.getIdentifier();
    }

    @Override
    public void cancel() {
        this.canceled = true;
        for (Future<?> queryExecution : this.queryExecutions) {
            queryExecution.cancel(true);
        }
        queryResult.cancel();
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    public Query getQuery() {
        return query;
    }

    @Override
    public StandardQueryResult getResult() {
        return queryResult;
    }

    public void addQueryExecution(Future<?> execution) {
        this.queryExecutions.add(execution);
    }
}
