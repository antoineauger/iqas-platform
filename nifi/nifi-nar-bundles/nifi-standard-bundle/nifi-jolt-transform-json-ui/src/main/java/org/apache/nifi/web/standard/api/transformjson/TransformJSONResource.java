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

package org.apache.nifi.web.standard.api.transformjson;

import java.io.File;
import java.io.FilenameFilter;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.nifi.processors.standard.util.TransformFactory;
import org.apache.nifi.web.standard.api.AbstractStandardResource;
import org.apache.nifi.web.standard.api.transformjson.dto.JoltSpecificationDTO;
import org.apache.nifi.web.standard.api.transformjson.dto.ValidationDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Transform;

@Path("/standard/transformjson")
public class TransformJSONResource extends AbstractStandardResource {

    private static final Logger logger = LoggerFactory.getLogger(TransformJSONResource.class);

    protected Object getSpecificationJsonObject(String specification){

        if (!StringUtils.isEmpty(specification)){
            return JsonUtils.jsonToObject(specification, "UTF-8");
        }else{
            return null;
        }
    }

    @POST
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/validate")
    public Response validateSpec(JoltSpecificationDTO specificationDTO) {

        try {
            getTransformation(specificationDTO);
        }catch(final Exception e){
            logger.error("Validation Failed - " + e.toString());
            return Response.ok(new ValidationDTO(false,"Validation Failed - Please verify the provided specification.")).build();
        }

        return Response.ok(new ValidationDTO(true,null)).build();
    }

    @POST
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/execute")
    public Response executeSpec(JoltSpecificationDTO specificationDTO) {

        try {
            Transform transform = getTransformation(specificationDTO);
            Object inputJson = JsonUtils.jsonToObject(specificationDTO.getInput());
            return Response.ok(JsonUtils.toJsonString(transform.transform(inputJson))).build();

        }catch(final Exception e){
            logger.error("Execute Specification Failed - " + e.toString());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }

    }

    protected Transform getTransformation(JoltSpecificationDTO specificationDTO) throws Exception{

        Object specJson = getSpecificationJsonObject(specificationDTO.getSpecification());
        String transformName = specificationDTO.getTransform();
        String modules = specificationDTO.getModules();

        ClassLoader classLoader = null;
        Transform transform ;

        if(modules != null && !modules.isEmpty()){
            classLoader = ClassLoaderUtils.getCustomClassLoader(specificationDTO.getModules(),this.getClass().getClassLoader(), getJarFilenameFilter());
        } else{
            classLoader = this.getClass().getClassLoader();
        }

        if(transformName.equals("jolt-transform-custom")) {
            transform = TransformFactory.getCustomTransform(classLoader,specificationDTO.getCustomClass(), specJson);
        }else{
            transform = TransformFactory.getTransform(classLoader,specificationDTO.getTransform(), specJson);
        }

        return transform;
    }

    protected FilenameFilter getJarFilenameFilter(){
        return  new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return (name != null && name.endsWith(".jar"));
            }
        };
    }

}
