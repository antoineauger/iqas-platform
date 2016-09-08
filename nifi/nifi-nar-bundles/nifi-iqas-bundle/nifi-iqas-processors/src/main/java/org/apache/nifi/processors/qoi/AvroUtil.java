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
package org.apache.nifi.processors.qoi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroUtil {

    /**
     *
     * @param avroTYpe
     * @param avroName
     * @param avroNamespace
     * @param fieldsToPreserve
     * @return
     */
    public static Schema buildGlobalSchema(String avroTYpe, String avroName, String avroNamespace, List<Schema.Field> fieldsToPreserve) {
        Schema schemaToReturn;

        String stringNewSchema = "{\"type\":\""+ avroTYpe +"\",";
        if (avroNamespace != null) {
            stringNewSchema += "\"namespace\":\""+ avroNamespace +"\",";
        }
        stringNewSchema += "\"name\":\""+ avroName +"\",\"fields\":[";
        for (Schema.Field f : fieldsToPreserve) {
            if (!f.name().equals("qoi") && f.schema() != null) {
                stringNewSchema += "{\"name\":\"" + f.name() + "\",\"type\":\"" + f.schema().getType().toString().toLowerCase() + "\"},";
            }
        }
        stringNewSchema += "{\"name\":\"qoi\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"qoi_record\",\"fields\":[{\"name\":\"checkpointName\",\"type\":\"string\"},{\"name\":\"qoi_attr\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}}}]}";

        schemaToReturn = new Schema.Parser().parse(stringNewSchema);
        return schemaToReturn;
    }

    /**
     *
     * @return
     */
    public static Schema buildQoISchema(ComponentLog log, boolean underArrayFormat) {
        Schema schemaToReturn;
        String stringNewSchema;

        if (underArrayFormat) {
            // For a proper schema, see qoi_array.avsc file
            stringNewSchema = "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"qoi_record\",\"fields\":[{\"name\":\"checkpointName\",\"type\":\"string\"},{\"name\":\"qoi_attr\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}}";
        }
        else {
            // For a proper schema, see qoi_record.avsc file
            stringNewSchema = "{\"type\":\"record\",\"name\":\"qoi_record\",\"fields\":[{\"name\":\"checkpointName\",\"type\":\"string\"},{\"name\":\"qoi_attr\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}";
        }

        schemaToReturn = new Schema.Parser().parse(stringNewSchema);
        return schemaToReturn;
    }

    /**
     *
     * @param oldRecord
     * @param fieldsToPreserve
     * @param schemaForNewRecord
     * @return
     */
    public static GenericRecord copyFields(GenericRecord oldRecord, List<Schema.Field> fieldsToPreserve, Schema schemaForNewRecord) {
        GenericRecord recordToReturn = new GenericData.Record(schemaForNewRecord);

        for (Schema.Field f : fieldsToPreserve) {
            recordToReturn.put(f.name(), oldRecord.get(f.name()));
        }

        return recordToReturn;
    }

    /**
     *
     * @param record
     * @param checkpointName
     * @param qoiAttributes
     * @return
     */
    public static GenericRecord annotateRecordWithQoIAttr(ComponentLog log, GenericRecord record, String checkpointName, Map<String, String> qoiAttributes) {
        GenericRecord recordToReturn = record;
        List<GenericRecord> list = new ArrayList<>();
        Map<String, String> map = new HashMap<>(qoiAttributes);

        // Writing of the QoI attributes
        GenericRecord qoiRecord = new GenericData.Record(AvroUtil.buildQoISchema(log, false));
        qoiRecord.put("checkpointName",checkpointName);
        qoiRecord.put("qoi_attr",map);

        if (recordToReturn.get("qoi") != null) {
            list = (List<GenericRecord>) record.get("qoi");
            list.add(qoiRecord);
        }
        else {
            list.add(qoiRecord);
        }
        recordToReturn.put("qoi",list);

        return recordToReturn;
    }
}
