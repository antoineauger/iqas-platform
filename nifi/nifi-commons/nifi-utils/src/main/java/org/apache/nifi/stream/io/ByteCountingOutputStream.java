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
package org.apache.nifi.stream.io;

import java.io.IOException;
import java.io.OutputStream;

public class ByteCountingOutputStream extends OutputStream {

    private final OutputStream out;
    private long bytesWritten = 0L;

    public ByteCountingOutputStream(final OutputStream out) {
        this.out = out;
    }

    public ByteCountingOutputStream(final OutputStream out, final long initialByteCount) {
        this.out = out;
        this.bytesWritten = initialByteCount;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        bytesWritten++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        bytesWritten += len;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    public OutputStream getWrappedStream() {
        return out;
    }
}
