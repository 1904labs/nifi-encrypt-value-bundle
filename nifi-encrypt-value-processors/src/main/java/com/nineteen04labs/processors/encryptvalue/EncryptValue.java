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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.nineteen04labs.processors.util.Encryption;
import com.nineteen04labs.processors.util.FormatStream;

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
import org.apache.nifi.processor.io.StreamCallback;

@Tags({"encrypt", "hash", "json", "pii"})
@CapabilityDescription("Encrypts the values of the given fields of a FlowFile. The original value is replaced with the hashed one.")
public class EncryptValue extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(EncryptValueProperties.FLOW_FORMAT);
        descriptors.add(EncryptValueProperties.AVRO_SCHEMA);
        descriptors.add(EncryptValueProperties.FIELD_NAMES);
        descriptors.add(EncryptValueProperties.HASH_ALG);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(EncryptValueRelationships.REL_SUCCESS);
        relationships.add(EncryptValueRelationships.REL_FAILURE);
        relationships.add(EncryptValueRelationships.REL_BYPASS);
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
            final String rawFieldNames = context.getProperty(EncryptValueProperties.FIELD_NAMES).getValue();
            if (rawFieldNames == null) {
                session.transfer(flowFile, EncryptValueRelationships.REL_BYPASS);
                return;
            }
            final List<String> fieldNames = Arrays.asList(rawFieldNames.split(","));
            final String flowFormat = context.getProperty(EncryptValueProperties.FLOW_FORMAT).getValue();
            final String schemaString = context.getProperty(EncryptValueProperties.AVRO_SCHEMA).getValue();
            final String algorithm = context.getProperty(EncryptValueProperties.HASH_ALG).getValue();
            
            session.write(flowFile, new StreamCallback(){
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    JsonFactory jsonFactory = new JsonFactory().setRootValueSeparator(null);

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();

                    JsonParser jsonParser;
                    JsonGenerator jsonGen = jsonFactory.createGenerator(baos);

                    if (flowFormat == "AVRO")
                        in = FormatStream.avroToJson(in, schemaString);

                    Reader r = new InputStreamReader(in);
                    BufferedReader br = new BufferedReader(r);
                    String line;

                    while ((line = br.readLine()) != null) {
                        jsonParser = jsonFactory.createParser(line);
                        while (jsonParser.nextToken() != null) {
                            jsonGen.copyCurrentEvent(jsonParser);
                            if(fieldNames.contains(jsonParser.getCurrentName())) {
                                jsonParser.nextToken();
                                String hashedValue = Encryption.hashValue(jsonParser.getText(), algorithm);
                                jsonGen.writeString(hashedValue);
                            }
                        }
                        jsonGen.writeRaw("\n");
                    }
                    jsonGen.flush();

                    if (flowFormat == "AVRO")
                        baos = FormatStream.jsonToAvro(baos, schemaString);

                    baos.writeTo(out);
                }
            });
            
            session.transfer(flowFile, EncryptValueRelationships.REL_SUCCESS);

        } catch (ProcessException e) {
            getLogger().error("Something went wrong", e);
            session.transfer(flowFile, EncryptValueRelationships.REL_FAILURE);
        }
    }
}
