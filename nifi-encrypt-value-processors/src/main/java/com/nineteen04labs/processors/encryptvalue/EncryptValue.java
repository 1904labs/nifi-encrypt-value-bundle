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
package com.nineteen04labs.processors.encryptvalue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Provider.Service;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.security.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"encrypt", "hash", "json", "pii"})
@CapabilityDescription("Encrypts the values of the given fields of a FlowFile. The original value is replaced with the hashed one.")
public class EncryptValue extends AbstractProcessor {

    public static final PropertyDescriptor FLOW_FORMAT = new PropertyDescriptor
            .Builder().name("FLOW_FORMAT")
            .displayName("FlowFile Format")
            .description("Specify the format of the incoming FlowFile")
            .required(true)
            // TODO: Use a Set with at least JSON and AVRO as allowable values
            .allowableValues("JSON")
            .defaultValue("JSON")
            .build();

    public static final PropertyDescriptor FIELD_NAMES = new PropertyDescriptor
            .Builder().name("FIELD_NAMES")
            .displayName("Field Names")
            .description("Comma separated list of fields whose values to encrypt.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor HASH_ALG = new PropertyDescriptor
            .Builder().name("HASH_ALG")
            .displayName("Hash Algorithm")
            .description("Determines what hashing algorithm should be used to perform the encryption")
            .required(true)
            .allowableValues(getAvailableAlgorithms())
            .defaultValue("SHA-512")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are processed successfully will be sent to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that cannot be processed successfully will be sent to this relationship")
            .build();

    public static Set<String> getAvailableAlgorithms() {
        final String digestClassName = MessageDigest.class.getSimpleName();
        final Set<String> algorithms = new TreeSet<>();
        
        for (Provider prov : Security.getProviders()) {
            prov.getServices().stream()
                .filter(s -> digestClassName.equalsIgnoreCase(s.getType()))
                .map(Service::getAlgorithm)
                .collect(Collectors.toCollection(() -> algorithms));
        }
        return algorithms;
    }

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(FLOW_FORMAT);
        descriptors.add(FIELD_NAMES);
        descriptors.add(HASH_ALG);
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {        
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        try {
            final AtomicReference<String> contentRef = new AtomicReference<>();

            session.read(flowFile, new InputStreamCallback(){
                @Override
                public void process(InputStream in) throws IOException {
                    contentRef.set(IOUtils.toString(in, StandardCharsets.UTF_8));
                }
            });
            String content = contentRef.get();
            
            String rawFieldNames = context.getProperty(FIELD_NAMES).getValue();
            List<String> fieldNames = new ArrayList<String>();
            if (rawFieldNames == null) {
                session.transfer(flowFile, REL_SUCCESS);
                return;
            } else {
                fieldNames = Arrays.asList(rawFieldNames.split(","));
            }

            String algorithm = context.getProperty(HASH_ALG).getValue();
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            
            @SuppressWarnings("unchecked")
            Map<String,Object> contentMap = new ObjectMapper().readValue(content, LinkedHashMap.class);

            for(String fieldName : fieldNames) {
                if (contentMap.containsKey(fieldName)) {
                    String valueToHash = contentMap.get(fieldName).toString();
                    byte[] hash = digest.digest(valueToHash.getBytes(StandardCharsets.UTF_8));

                    StringBuffer buffer = new StringBuffer();
                    for (byte b : hash) {
                        buffer.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
                    }

                    String hashedValue = buffer.toString();
                    contentMap.replace(fieldName, valueToHash, hashedValue);
                }
            }

            String newContent = new ObjectMapper().writeValueAsString(contentMap);           

            flowFile = session.write(flowFile, outputStream -> outputStream.write(newContent.getBytes()));

            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            getLogger().error("Something went wrong", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
