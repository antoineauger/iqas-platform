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

public class OrEvaluator extends BooleanEvaluator {

    private final Evaluator<Boolean> subjectEvaluator;
    private final Evaluator<Boolean> rhsEvaluator;

    public OrEvaluator(final Evaluator<Boolean> subjectEvaluator, final Evaluator<Boolean> rhsEvaluator) {
        this.subjectEvaluator = subjectEvaluator;
        this.rhsEvaluator = rhsEvaluator;
    }

    @Override
    public QueryResult<Boolean> evaluate(final Map<String, String> attributes) {
        final QueryResult<Boolean> subjectValue = subjectEvaluator.evaluate(attributes);
        if (subjectValue == null) {
            return new BooleanQueryResult(null);
        }

        if (Boolean.TRUE.equals(subjectValue.getValue())) {
            return new BooleanQueryResult(true);
        }

        final QueryResult<Boolean> rhsValue = rhsEvaluator.evaluate(attributes);
        if (rhsValue == null) {
            return new BooleanQueryResult(false);
        }

        return new BooleanQueryResult(rhsValue.getValue());
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }

}
