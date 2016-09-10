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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroUtil {

    private Schema qoIArray;
    private Schema qoIRecord;

    public AvroUtil() {
        try {
            qoIArray = new Schema.Parser().parse(getClass().getResourceAsStream("/avro_schemas/qoi_array.avsc"));
            qoIRecord = new Schema.Parser().parse(getClass().getResourceAsStream("/avro_schemas/qoi_record.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param avroTYpe
     * @param avroName
     * @param avroNamespace
     * @param fieldsToPreserve
     * @return
     */
    public Schema buildGlobalSchema(String avroTYpe, String avroName, String avroNamespace, List<Schema.Field> fieldsToPreserve) {
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
        stringNewSchema += "{\"name\":\"qoi\",\"type\":" + qoIArray.toString().replaceAll("\"","\"") + "}]}";

        schemaToReturn = new Schema.Parser().parse(stringNewSchema);
        return schemaToReturn;
    }

    /**
     *
     * @return
     */
    public Schema buildQoISchema(boolean underArrayFormat) {
        Schema schemaToReturn;

        if (underArrayFormat) {
            // For a proper schema, see qoi_array.avsc file
            schemaToReturn = qoIArray;
        }
        else {
            // For a proper schema, see qoi_record.avsc file
            schemaToReturn = qoIRecord;
        }

        return schemaToReturn;
    }

    /**
     *
     * @param oldRecord
     * @param fieldsToPreserve
     * @param schemaForNewRecord
     * @return
     */
    public GenericRecord copyFields(GenericRecord oldRecord, List<Schema.Field> fieldsToPreserve, Schema schemaForNewRecord) {
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
    public GenericRecord annotateRecordWithQoIAttr(GenericRecord record, String checkpointName, Map<String, String> qoiAttributes) {
        GenericRecord recordToReturn = record;
        List<GenericRecord> list = new ArrayList<>();
        Map<String, String> map = new HashMap<>(qoiAttributes);

        // Writing of the QoI attributes
        GenericRecord qoiRecord = new GenericData.Record(buildQoISchema(false));
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
