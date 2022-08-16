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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.rocketmq.flink.common.constant.SchemaRegistryConstant;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class RocketMQCatalogTest {

    @Mock
    private SchemaRegistryClient schemaRegistryClient;
    @Mock
    private GetSchemaResponse getSchemaResponse;

    private RocketMQCatalog rocketMQCatalog;

    @Before
    public void setUp() throws Exception {
        rocketMQCatalog = new RocketMQCatalog("rocketmq-catalog", "default",
            "http://localhost:8080", SchemaRegistryConstant.SCHEMA_REGISTRY_BASE_URL);

        Field declaredField = rocketMQCatalog.getClass().getDeclaredField("schemaRegistryClient");
        declaredField.setAccessible(true);
        declaredField.set(rocketMQCatalog, schemaRegistryClient);

        List<String> list = new ArrayList();
        list.add("test");
        Mockito.when(schemaRegistryClient.getSubjectsByTenant(null, "default")).thenReturn(list);

        Mockito.when(schemaRegistryClient.getSchemaBySubject("test")).thenReturn(getSchemaResponse);
        Mockito.when(getSchemaResponse.getType()).thenReturn(SchemaType.AVRO);
        Mockito.when(getSchemaResponse.getIdl()).thenReturn("{\"type\":\"record\",\"name\":\"Charge\"," +
            "\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\",\"fields\":[{\"name\":\"item\"," +
            "\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}");
    }

    @Test
    public void testGetFactory() {
        Optional<Factory> factory = rocketMQCatalog.getFactory();
        assertNotNull(factory.get());
    }

    @Test
    public void testOpen() throws NoSuchFieldException, IllegalAccessException {
        rocketMQCatalog.open();

        Class<? extends RocketMQCatalog> aClass = rocketMQCatalog.getClass();
        Field mqAdminExtField = aClass.getDeclaredField("mqAdminExt");
        mqAdminExtField.setAccessible(true);
        Field schemaRegistryClientField = aClass.getDeclaredField("schemaRegistryClient");
        schemaRegistryClientField.setAccessible(true);

        Object mqAdminExt = mqAdminExtField.get(rocketMQCatalog);
        Object schemaRegistryClient = schemaRegistryClientField.get(rocketMQCatalog);
        assertNotNull(mqAdminExt);
        assertNotNull(schemaRegistryClient);
    }

    @Test
    public void testClose() throws NoSuchFieldException, IllegalAccessException {
        rocketMQCatalog.close();

        Class<? extends RocketMQCatalog> aClass = rocketMQCatalog.getClass();
        Field mqAdminExtField = aClass.getDeclaredField("mqAdminExt");
        mqAdminExtField.setAccessible(true);
        Field schemaRegistryClientField = aClass.getDeclaredField("schemaRegistryClient");
        schemaRegistryClientField.setAccessible(true);

        Object mqAdminExt = mqAdminExtField.get(rocketMQCatalog);
        Object schemaRegistryClient = schemaRegistryClientField.get(rocketMQCatalog);
        assertNull(mqAdminExt);
        assertNull(schemaRegistryClient);
    }

    @Test
    public void testListDatabases() {
        List<String> strings = rocketMQCatalog.listDatabases();
        assertEquals(1, strings.size());
        assertEquals("default", strings.get(0));
    }

    @Test
    public void testGetDatabase() throws DatabaseNotExistException {
        CatalogDatabase database = rocketMQCatalog.getDatabase("default");
        assertNotNull(database);
    }

    @Test
    public void testDatabaseExists() {
        boolean exists = rocketMQCatalog.databaseExists("default");
        assertTrue(exists);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateDatabase() throws DatabaseAlreadyExistException {
        rocketMQCatalog.createDatabase("test", null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDropDatabase() throws DatabaseNotEmptyException, DatabaseNotExistException {
        rocketMQCatalog.dropDatabase("test", false, false);
    }

    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> strings = rocketMQCatalog.listTables("default");
        assertEquals(1, strings.size());
        assertEquals("test", strings.get(0));
    }

    @Test
    public void testGetTable() throws TableNotExistException {
        ObjectPath objectPath = new ObjectPath("default", "test");
        CatalogBaseTable catalogBaseTable = rocketMQCatalog.getTable(objectPath);
        assertNotNull(catalogBaseTable);
    }

    @Test
    public void testTableExists() {
        ObjectPath objectPath = new ObjectPath("default", "test");
        boolean exists = rocketMQCatalog.tableExists(objectPath);
        assertTrue(exists);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateTable() throws TableAlreadyExistException, DatabaseNotExistException {
        rocketMQCatalog.createTable(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDropTable() throws TableNotExistException {
        rocketMQCatalog.dropTable(null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testListFunctions() throws DatabaseNotExistException {
        rocketMQCatalog.listFunctions("default");
    }

    @Test(expected = FunctionNotExistException.class)
    public void testGetFunction() throws FunctionNotExistException {
        ObjectPath objectPath = new ObjectPath("default", "test");
        rocketMQCatalog.getFunction(objectPath);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFunctionExists() {
        boolean exists = rocketMQCatalog.functionExists(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateFunction() throws FunctionAlreadyExistException, DatabaseNotExistException {
        rocketMQCatalog.createFunction(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterFunction() throws FunctionNotExistException {
        rocketMQCatalog.alterFunction(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDropFunction() throws FunctionNotExistException {
        rocketMQCatalog.dropFunction(null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterDatabase() throws DatabaseNotExistException {
        rocketMQCatalog.alterDatabase(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testListViews() throws DatabaseNotExistException {
        rocketMQCatalog.listViews("default");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterTable() throws TableNotExistException {
        rocketMQCatalog.alterTable(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRenameTable() throws TableAlreadyExistException, TableNotExistException {
        rocketMQCatalog.renameTable(null, null, false);
    }

    @Test
    public void testListPartitions() {
    }

    @Test
    public void testListPartitionsByFilter() {
    }

    @Test
    public void testGetPartition() {
    }

    @Test
    public void testPartitionExists() {
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreatePartition() throws TableNotPartitionedException, TableNotExistException,
        PartitionSpecInvalidException, PartitionAlreadyExistsException {
        rocketMQCatalog.createPartition(null, null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDropPartition() throws PartitionNotExistException {
        rocketMQCatalog.dropPartition(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterPartition() throws PartitionNotExistException {
        rocketMQCatalog.alterPartition(null, null, null, false);
    }

    @Test
    public void testGetTableStatistics() throws TableNotExistException {
        CatalogTableStatistics statistics = rocketMQCatalog.getTableStatistics(null);
        assertEquals(statistics, CatalogTableStatistics.UNKNOWN);
    }

    @Test
    public void testGetTableColumnStatistics() throws TableNotExistException {
        CatalogColumnStatistics statistics = rocketMQCatalog.getTableColumnStatistics(null);
        assertEquals(statistics, CatalogColumnStatistics.UNKNOWN);
    }

    @Test
    public void testGetPartitionStatistics() throws PartitionNotExistException {
        CatalogTableStatistics statistics = rocketMQCatalog.getPartitionStatistics(null, null);
        assertEquals(statistics, CatalogTableStatistics.UNKNOWN);
    }

    @Test
    public void testGetPartitionColumnStatistics() throws PartitionNotExistException {
        CatalogColumnStatistics statistics = rocketMQCatalog.getPartitionColumnStatistics(null, null);
        assertEquals(statistics, CatalogColumnStatistics.UNKNOWN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterTableStatistics() throws TableNotExistException {
        rocketMQCatalog.alterTableStatistics(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterTableColumnStatistics() throws TableNotExistException {
        rocketMQCatalog.alterTableColumnStatistics(null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterPartitionStatistics() throws PartitionNotExistException {
        rocketMQCatalog.alterPartitionStatistics(null, null, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAlterPartitionColumnStatistics() throws PartitionNotExistException {
        rocketMQCatalog.alterPartitionColumnStatistics(null, null, null, false);
    }
}