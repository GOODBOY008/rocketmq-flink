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

package org.apache.rocketmq.flink.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.IDENTIFIER;
import static org.apache.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.NAME_SERVER_ADDR;
import static org.apache.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.SCHEMA_REGISTRY_BASE_URL;

/** The {@CatalogFactory} implementation of RocketMQ. */
public class RocketMQCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        return new RocketMQCatalog(
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(NAME_SERVER_ADDR),
                helper.getOptions().get(SCHEMA_REGISTRY_BASE_URL));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        return options;
    }
}
