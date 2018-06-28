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
package com.nineteen04labs.processors.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Provider.Service;
import java.security.Security;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class Encryption {

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

    public static String hashValue(String valueToHash, String algorithm) {
        try{
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] hash = digest.digest(valueToHash.getBytes(StandardCharsets.UTF_8));
            StringBuffer buffer = new StringBuffer();
            for (byte b : hash) {
                buffer.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
            return buffer.toString();
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            return null;
        }
    }
    
}
