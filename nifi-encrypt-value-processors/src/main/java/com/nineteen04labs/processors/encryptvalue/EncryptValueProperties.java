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

import com.nineteen04labs.processors.util.Encryption;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class EncryptValueProperties {

    public static final PropertyDescriptor FLOW_FORMAT = new PropertyDescriptor
            .Builder().name("FLOW_FORMAT")
            .displayName("FlowFile Format")
            .description("Specify the format of the incoming FlowFile. If AVRO, output is automatically Snappy compressed.")
            .required(true)
            .allowableValues("JSON", "AVRO")
            .defaultValue("JSON")
            .build();

    public static final PropertyDescriptor AVRO_SCHEMA = new PropertyDescriptor
            .Builder().name("AVRO_SCHEMA")
            .displayName("Avro Schema")
            .description("Specify the schema if the FlowFile format is Avro.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
            .allowableValues(Encryption.getAvailableAlgorithms())
            .defaultValue("SHA-512")
            .build();

}
