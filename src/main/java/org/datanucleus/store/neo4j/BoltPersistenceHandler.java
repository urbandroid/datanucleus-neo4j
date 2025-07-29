/**********************************************************************
Copyright (c) 2024 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package org.datanucleus.store.neo4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.neo4j.fieldmanager.BoltFieldManager;
import org.datanucleus.store.neo4j.fieldmanager.BoltFetchFieldManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;

public class BoltPersistenceHandler extends AbstractPersistenceHandler {

    public BoltPersistenceHandler(StoreManager storeMgr) {
        super(storeMgr);
    }

    @Override
    public void close() {}

    @Override
    public void insertObject(DNStateManager sm) {
        assertReadOnlyForUpdateOfObject(sm);
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);

        try {
            Transaction tx = ((BoltConnectionFactoryImpl.EmulatedXAResource) mconn.getXAResource()).getTransaction();
            insertObjectAndCacheNode(sm, tx);

            // Now handle relations using the cached Node
            Node node = (Node) sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
            int[] relPositions = sm.getClassMetaData().getRelationMemberPositions(ec.getClassLoaderResolver());
            if (relPositions.length > 0) {
                sm.provideFields(relPositions, new BoltFieldManager(sm, tx, true, node));
            }
        } finally {
            mconn.release();
        }
    }

    private void insertObjectAndCacheNode(DNStateManager sm, Transaction tx) {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();

        Map<String, Object> props = new HashMap<>();
        if (cmd.isVersioned()) {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getStrategy() == VersionStrategy.VERSION_NUMBER) {
                long version = 1L;
                sm.setTransactionalVersion(version);
                if (vermd.getMemberName() != null) {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getMemberName());
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), version);
                } else {
                    props.put(Neo4jSchemaUtils.getSurrogateVersionName(cmd, storeMgr), version);
                }
            }
        }
        if (cmd.hasDiscriminatorStrategy()) {
            props.put(Neo4jSchemaUtils.getSurrogateDiscriminatorName(cmd, storeMgr), cmd.getDiscriminatorValue());
        }

        BoltFieldManager fm = new BoltFieldManager(sm, tx, true);
        sm.provideFields(cmd.getNonRelationMemberPositions(ec.getClassLoaderResolver()), fm);
        props.putAll(fm.getProperties());

        List<String> labelList = Neo4jSchemaUtils.getLabelsForClass(cmd, storeMgr);
        StringBuilder cypher = new StringBuilder("CREATE (n");
        labelList.forEach(label -> cypher.append(":").append(Neo4jSchemaUtils.getLabelName(label)));
        cypher.append(" $props) RETURN n, id(n) as nodeId");

        Result result = tx.run(cypher.toString(), Values.parameters("props", props));
        if (result.hasNext()) {
            org.neo4j.driver.Record record = result.next();
            Node node = record.get("n").asNode();
            long nodeId = record.get("nodeId").asLong();
            
            sm.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, node);
            
            if (cmd.pkIsDatastoreAttributed(storeMgr)) {
                sm.setPostStoreNewObjectId(nodeId);
            }
        } else {
            throw new NucleusDataStoreException("CREATE query failed for: " + sm.getObjectAsPrintable());
        }
    }

    @Override
    public void fetchObject(DNStateManager sm, int[] fieldNumbers) {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try {
            Transaction tx = ((BoltConnectionFactoryImpl.EmulatedXAResource) mconn.getXAResource()).getTransaction();
            
            Node node = BoltPersistenceUtils.getPropertyContainerForStateManager(tx, sm);
            if (node == null) {
                throw new NucleusObjectNotFoundException("Datastore object not found for id: " + sm.getInternalObjectId());
            }

            sm.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, node);
            
            sm.replaceFields(fieldNumbers, new BoltFetchFieldManager(sm, tx, node));

            if (sm.getClassMetaData().isVersioned() && sm.getTransactionalVersion() == null) {
                VersionMetaData vermd = sm.getClassMetaData().getVersionMetaDataForClass();
                Object datastoreVersion = null;
                if (vermd.getMemberName() != null) {
                    datastoreVersion = sm.provideField(sm.getClassMetaData().getAbsolutePositionOfMember(vermd.getMemberName()));
                } else {
                    String versionName = Neo4jSchemaUtils.getSurrogateVersionName(sm.getClassMetaData(), storeMgr);
                    if (node.containsKey(versionName)) {
                        Value versionValue = node.get(versionName);
                        if (versionValue != null && !versionValue.isNull()) {
                             datastoreVersion = versionValue.asObject();
                        }
                    }
                }
                sm.setVersion(datastoreVersion);
            }
        } finally {
            mconn.release();
        }
    }

    @Override
    public void updateObject(DNStateManager sm, int[] fieldNumbers) {
        // Implementation for update
    }

    @Override
    public void deleteObject(DNStateManager sm) {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try {
            Transaction tx = ((BoltConnectionFactoryImpl.EmulatedXAResource) mconn.getXAResource()).getTransaction();
            Node node = BoltPersistenceUtils.getPropertyContainerForStateManager(tx, sm);

            if (node == null) {
                return;
            }
            sm.loadUnloadedFields();
            sm.provideFields(sm.getClassMetaData().getRelationMemberPositions(ec.getClassLoaderResolver()), new BoltFieldManager(sm, tx, false, node));
            tx.run("MATCH (n) WHERE id(n) = $id DETACH DELETE n", Values.parameters("id", node.id()));
        } finally {
            mconn.release();
        }
    }
    
    @Override
    public void locateObject(DNStateManager sm) {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try {
            Transaction tx = ((BoltConnectionFactoryImpl.EmulatedXAResource) mconn.getXAResource()).getTransaction();
            if (BoltPersistenceUtils.getPropertyContainerForStateManager(tx, sm) == null) {
                throw new NucleusObjectNotFoundException("Object not found for id: " + sm.getInternalObjectId());
            }
        } finally {
            mconn.release();
        }
    }
    
    @Override
    public Object findObject(ExecutionContext ec, Object id) {
        return null;
    }
}