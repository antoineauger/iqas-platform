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
package org.apache.nifi.attribute.expression.language.evaluation.functions;

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;

public class EndsWithEvaluator extends BooleanEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> search;

    public EndsWithEvaluator(final Evaluator<String> subject, final Evaluator<String> search) {
        this.subject = subject;
        this.search = search;
    }

    @Override
    public QueryResult<Boolean> evaluate(final Map<String, String> attributes) {
        final String subjectValue = subject.evaluate(attributes).getValue();
        if (subjectValue == null) {
            return new BooleanQueryResult(false);
        }

        final String searchString = search.evaluate(attributes).getValue();
        return new BooleanQueryResult(searchString == null ? false : subjectValue.endsWith(searchString));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
