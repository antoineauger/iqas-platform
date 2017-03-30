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

public final class StandardFlowFileEvent implements FlowFileEvent, Cloneable {

    private final String componentId;

    private int flowFilesIn;
    private int flowFilesOut;
    private int flowFilesRemoved;
    private long contentSizeIn;
    private long contentSizeOut;
    private long contentSizeRemoved;
    private long bytesRead;
    private long bytesWritten;
    private long processingNanos;
    private long aggregateLineageMillis;
    private int flowFilesReceived;
    private long bytesReceived;
    private int flowFilesSent;
    private long bytesSent;
    private int invocations;

    public StandardFlowFileEvent(final String componentId) {
        this.componentId = componentId;
    }

    public StandardFlowFileEvent(final String componentId,
            final int flowFilesIn, final long contentSizeIn,
            final int flowFilesOut, final long contentSizeOut,
            final int flowFilesRemoved, final long contentSizeRemoved,
            final long bytesRead, final long bytesWritten,
            final int flowFilesReceived, final long bytesReceived,
            final int flowFilesSent, final long bytesSent,
            final int invocations, final long averageLineageMillis, final long processingNanos) {
        this.componentId = componentId;
        this.flowFilesIn = flowFilesIn;
        this.contentSizeIn = contentSizeIn;
        this.flowFilesOut = flowFilesOut;
        this.contentSizeOut = contentSizeOut;
        this.flowFilesRemoved = flowFilesRemoved;
        this.contentSizeRemoved = contentSizeRemoved;
        this.bytesRead = bytesRead;
        this.bytesWritten = bytesWritten;
        this.invocations = invocations;
        this.flowFilesReceived = flowFilesReceived;
        this.bytesReceived = bytesReceived;
        this.flowFilesSent = flowFilesSent;
        this.bytesSent = bytesSent;
        this.aggregateLineageMillis = averageLineageMillis;
        this.processingNanos = processingNanos;
    }

    public StandardFlowFileEvent(final FlowFileEvent other) {
        this.componentId = other.getComponentIdentifier();
        this.flowFilesIn = other.getFlowFilesIn();
        this.contentSizeIn = other.getContentSizeIn();
        this.flowFilesOut = other.getFlowFilesOut();
        this.contentSizeOut = other.getContentSizeOut();
        this.flowFilesRemoved = other.getFlowFilesRemoved();
        this.contentSizeRemoved = other.getContentSizeRemoved();
        this.bytesRead = other.getBytesRead();
        this.bytesWritten = other.getBytesWritten();
        this.invocations = other.getInvocations();
        this.flowFilesReceived = other.getFlowFilesReceived();
        this.bytesReceived = other.getBytesReceived();
        this.flowFilesSent = other.getFlowFilesSent();
        this.bytesSent = other.getBytesSent();
        this.aggregateLineageMillis = other.getAggregateLineageMillis();
        this.processingNanos = other.getProcessingNanoseconds();
    }

    @Override
    public String getComponentIdentifier() {
        return componentId;
    }

    @Override
    public int getFlowFilesIn() {
        return flowFilesIn;
    }

    public void setFlowFilesIn(int flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
    }

    @Override
    public int getFlowFilesOut() {
        return flowFilesOut;
    }

    public void setFlowFilesOut(int flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
    }

    @Override
    public long getContentSizeIn() {
        return contentSizeIn;
    }

    public void setContentSizeIn(long contentSizeIn) {
        this.contentSizeIn = contentSizeIn;
    }

    @Override
    public long getContentSizeOut() {
        return contentSizeOut;
    }

    public void setContentSizeOut(long contentSizeOut) {
        this.contentSizeOut = contentSizeOut;
    }

    @Override
    public long getContentSizeRemoved() {
        return contentSizeRemoved;
    }

    public void setContentSizeRemoved(final long contentSizeRemoved) {
        this.contentSizeRemoved = contentSizeRemoved;
    }

    @Override
    public int getFlowFilesRemoved() {
        return flowFilesRemoved;
    }

    public void setFlowFilesRemoved(final int flowFilesRemoved) {
        this.flowFilesRemoved = flowFilesRemoved;
    }

    @Override
    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    @Override
    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    @Override
    public long getProcessingNanoseconds() {
        return processingNanos;
    }

    public void setProcessingNanos(final long processingNanos) {
        this.processingNanos = processingNanos;
    }

    @Override
    public int getInvocations() {
        return invocations;
    }

    public void setInvocations(final int invocations) {
        this.invocations = invocations;
    }

    @Override
    public int getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public void setFlowFilesReceived(int flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
    }

    @Override
    public long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    @Override
    public int getFlowFilesSent() {
        return flowFilesSent;
    }

    public void setFlowFilesSent(int flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
    }

    @Override
    public long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(long bytesSent) {
        this.bytesSent = bytesSent;
    }

    @Override
    public long getAverageLineageMillis() {
        if (flowFilesOut == 0 && flowFilesRemoved == 0) {
            return 0L;
        }

        return aggregateLineageMillis / (flowFilesOut + flowFilesRemoved);
    }

    public void setAggregateLineageMillis(long lineageMilliseconds) {
        this.aggregateLineageMillis = lineageMilliseconds;
    }

    @Override
    public long getAggregateLineageMillis() {
        return aggregateLineageMillis;
    }

}
