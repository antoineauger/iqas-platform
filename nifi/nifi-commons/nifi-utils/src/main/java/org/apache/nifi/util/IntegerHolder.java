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
package org.apache.nifi.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @deprecated As of release 1.0.1. Please use {@link AtomicInteger}
 *
 * Wraps an Integer value so that it can be declared <code>final</code> and still be accessed from inner classes;
 * the functionality is similar to that of an AtomicInteger, but operations on this class
 * are not atomic. This results in greater performance when the atomicity is not needed.
 *
 */

@Deprecated
public class IntegerHolder extends ObjectHolder<Integer> {

    public IntegerHolder(final int initialValue) {
        super(initialValue);
    }

    public int addAndGet(final int delta) {
        final int curValue = get();
        final int newValue = curValue + delta;
        set(newValue);
        return newValue;
    }

    public int getAndAdd(final int delta) {
        final int curValue = get();
        final int newValue = curValue + delta;
        set(newValue);
        return curValue;
    }

    public int incrementAndGet() {
        return addAndGet(1);
    }

    public int getAndIncrement() {
        return getAndAdd(1);
    }

    public int decrementAndGet() {
        return addAndGet(-1);
    }

}
