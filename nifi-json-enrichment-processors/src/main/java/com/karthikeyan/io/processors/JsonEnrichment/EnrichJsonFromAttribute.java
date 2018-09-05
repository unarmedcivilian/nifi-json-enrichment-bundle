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
package com.karthikeyan.io.processors.JsonEnrichment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@SideEffectFree
@SupportsBatching
@Tags({"json","custom","enrich"})
@CapabilityDescription("Adds a list of attributes to json content of the flow file")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class EnrichJsonFromAttribute extends AbstractProcessor {

    private static final String AT_LIST_SEPARATOR = ",";

    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor
            .Builder().name("Attributes List")
            .description("Comma separated list of attributes to add to content")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully added attribute to flowfile content")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Could not add attribute to flowfile content")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private volatile Set<String> attributes;

    private Set<String> buildAtrs(String atrList) {
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                Set<String> result = new HashSet<>(ats.length);
                for (String str : ats) {
                    String trim = str.trim();
                        result.add(trim);
                }
                return result;
            }
        }
        return null;
    }

    protected Map<String, Object> buildAttributesMapForFlowFile(FlowFile ff, Map<String, Object> FlowFileMap, Set<String> attributes) {
        Map<String, Object> result;
        result = FlowFileMap;
            if(attributes != null) {
                for (String attribute : attributes) {
                    String val = ff.getAttribute(attribute);
                    if (val != null) {
                        result.put(attribute, val);
                    } else {
                        result.put(attribute, "");
                    }
                }
            }
            return result;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ATTRIBUTES_LIST);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        attributes = buildAtrs(context.getProperty(ATTRIBUTES_LIST).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final ObjectMapper mapper = new ObjectMapper();
        try {
        Map<String, Object> flowFileMap = mapper.readValue(session.read(flowFile), new TypeReference<HashMap<String, Object>>() {
        });

        Map<String, Object> ModifiedflowFileMap =  buildAttributesMapForFlowFile(flowFile, flowFileMap, attributes);
        flowFile = session.write(flowFile, (in, out) -> {
            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                outputStream.write(mapper.writeValueAsBytes(ModifiedflowFileMap));
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
        }
        catch (IOException ioe) {
        getLogger().error("IOException while reading JSON item: " + ioe.getMessage());
        session.transfer(flowFile, REL_FAILURE);
    }
        session.commit();
    }
}
