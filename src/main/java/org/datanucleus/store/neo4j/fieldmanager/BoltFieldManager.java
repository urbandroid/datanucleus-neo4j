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
package org.datanucleus.store.neo4j.fieldmanager;

import java.util.HashMap;
import java.util.Map;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node;

public class BoltFieldManager extends AbstractStoreFieldManager {

    private final Transaction tx;
    private final BoltRelationshipManager relMgr;
    private Map<String, Object> properties;
    private Map<AbstractMemberMetaData, Object> relationFields;
    private final Node node;

    public BoltFieldManager(DNStateManager sm, Transaction tx, boolean insert) {
        super(sm, insert);
        this.tx = tx;
        this.node = null;
        this.relMgr = new BoltRelationshipManager(sm, tx);
        if (insert) {
            this.properties = new HashMap<>();
            this.relationFields = new HashMap<>();
        }
    }

    /**
     * New constructor for update/delete/relation operations where the Node is already known.
     */
    public BoltFieldManager(DNStateManager sm, Transaction tx, boolean insert, Node node) {
        super(sm, insert);
        this.tx = tx;
        this.node = node;
        // IMPORTANT: Your BoltRelationshipManager class will need a matching constructor for this to work.
        this.relMgr = new BoltRelationshipManager(sm, tx, node);
        if (insert) {
            this.properties = new HashMap<>();
            this.relationFields = new HashMap<>();
        }
    }

    /**
     * Accessor for the properties map, used during the insert process.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public void storeObjectField(int fieldNumber, Object value) {
        if (!isStorable(fieldNumber)) return;
        AbstractMemberMetaData mmd = getMMD(fieldNumber);
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        boolean isRelation = (relationType != RelationType.NONE && !mmd.isEmbedded());

        if (insert) {
            if (isRelation) {
                if (value != null) relationFields.put(mmd, value);
            } else {
                if (properties != null) {
                    properties.put(mmd.getName(), value);
                }
            }
        } else { // DELETE or UPDATE LOGIC
            if (isRelation && value != null) {
                // When deleting, this finds and removes relationships.
                // When updating, this could be extended to update relationships.
                relMgr.deleteRelationField(mmd, value);
            } else if (!isRelation && node != null) {
                // Logic for updating a simple property on an existing node
                // Note: The new BoltPersistenceHandler handles this via a direct Cypher query now.
                // This path is primarily for relation management.
            }
        }
    }

    @Override public void storeBooleanField(int fn, boolean v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeCharField(int fn, char v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),String.valueOf(v)); }
    @Override public void storeByteField(int fn, byte v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeShortField(int fn, short v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeIntField(int fn, int v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeLongField(int fn, long v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeFloatField(int fn, float v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeDoubleField(int fn, double v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeStringField(int fn, String v) { if(insert && isStorable(fn) && properties != null) properties.put(getMMD(fn).getName(),v); }

    public void execute() {
        if (!insert) return;
        String label = cmd.getName();
        String cypher = String.format("CREATE (n:%s $props) RETURN id(n)", label);
        Result result = tx.run(cypher, Map.of("props", properties));
        if (result.hasNext()) {
            Record record = result.next();
            long nativeId = record.get(0).asLong();
            sm.setPostStoreNewObjectId(nativeId);
        }
        if (!relationFields.isEmpty()) {
            for (Map.Entry<AbstractMemberMetaData, Object> entry : relationFields.entrySet()) {
                relMgr.storeRelationField(entry.getKey(), entry.getValue());
            }
        }
    }
    
    private AbstractMemberMetaData getMMD(int fn) { return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fn); }
}