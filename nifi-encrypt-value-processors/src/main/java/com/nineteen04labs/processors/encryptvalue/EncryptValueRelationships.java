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

import org.apache.nifi.processor.Relationship;

public class EncryptValueRelationships {

        public static final Relationship REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("FlowFiles that are processed successfully will be sent to this relationship")
                .build();

        public static final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .description("FlowFiles that cannot be processed successfully will be sent to this relationship")
                .build();

        public static final Relationship REL_BYPASS = new Relationship.Builder()
                .name("bypass")
                .description("FlowFiles with a null 'Field Names' property will not be processed and sent to this relationship")
                .build();
}
