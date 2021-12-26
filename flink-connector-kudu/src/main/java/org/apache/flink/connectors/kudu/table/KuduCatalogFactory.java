/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.kudu.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for {@link KuduCatalog}.
 */
@Internal
public class KuduCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KuduCatalogFactory.class);

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("type", KuduTableFactory.KUDU);
        context.put("property-version", "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        properties.add(KuduTableFactory.KUDU_MASTERS);

        return properties;
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        return new KuduCatalog(name,
            descriptorProperties.getString(KuduTableFactory.KUDU_MASTERS));
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        descriptorProperties.validateString(KuduTableFactory.KUDU_MASTERS, false);
        return descriptorProperties;
    }

}
