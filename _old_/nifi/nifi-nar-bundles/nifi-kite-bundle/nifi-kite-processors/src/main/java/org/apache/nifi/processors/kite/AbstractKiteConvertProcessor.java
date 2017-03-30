/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import org.apache.avro.file.CodecFactory;
import org.apache.nifi.components.PropertyDescriptor;

import com.google.common.annotations.VisibleForTesting;

abstract class AbstractKiteConvertProcessor extends AbstractKiteProcessor {

    @VisibleForTesting
    static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("kite-compression-type")
            .displayName("Compression type")
            .description("Compression type to use when writting Avro files. Default is Snappy.")
            .allowableValues(CodecType.values())
            .defaultValue(CodecType.SNAPPY.toString())
            .build();

    public enum CodecType {
        BZIP2,
        DEFLATE,
        NONE,
        SNAPPY,
        LZO
    }

    protected CodecFactory getCodecFactory(String property) {
        CodecType type = CodecType.valueOf(property);
        switch (type) {
        case BZIP2:
            return CodecFactory.bzip2Codec();
        case DEFLATE:
            return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
        case NONE:
            return CodecFactory.nullCodec();
        case LZO:
            return CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
        case SNAPPY:
        default:
            return CodecFactory.snappyCodec();
        }
    }

}
