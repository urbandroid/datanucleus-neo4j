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

import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.DNStateManager;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node; // <-- REQUIRED IMPORT

import java.util.HashMap;
import java.util.Map;
import java.util.Collection;

public class BoltRelationshipManager {

    private final Transaction tx;
    private final DNStateManager sm;
    private final Node node; // <-- REQUIRED FIELD

    /**
     * Constructor for when the owner Node is already known (updates, deletes, and relation handling after insert).
     * @param sm The StateManager of the owner object.
     * @param tx The active transaction.
     * @param node The Neo4j Node representing the owner object.
     */
    public BoltRelationshipManager(DNStateManager sm, Transaction tx, Node node) {
        this.sm = sm;
        this.tx = tx;
        this.node = node;
    }

    /**
     * Constructor for the initial insert path before the owner Node is created.
     * @param sm The StateManager of the owner object.
     * @param tx The active transaction.
     */
    public BoltRelationshipManager(DNStateManager sm, Transaction tx) {
        this.sm = sm;
        this.tx = tx;
        this.node = null; // The node is not yet created in this path.
    }

    public void storeRelationField(AbstractMemberMetaData mmd, Object relatedObject) {
        if (relatedObject == null) {
            return;
        }
        if (relatedObject instanceof Collection) {
            for (Object element : (Collection<?>) relatedObject) {
                createRelationship(mmd, element);
            }
        } else {
            createRelationship(mmd, relatedObject);
        }
    }

    private void createRelationship(AbstractMemberMetaData mmd, Object relatedObject) {
        System.out.println("Relationship created!! ");
        ExecutionContext ec = sm.getExecutionContext();
        DNStateManager relatedSM = ec.findStateManager(relatedObject);
        if (relatedSM == null) {
            // Persist the related object if it's new
            relatedSM = ec.findStateManager(ec.persistObjectInternal(relatedObject, null, PersistableObjectType.PC, sm, mmd.getAbsoluteFieldNumber()));
        }
        
        // At this point, both owner and related objects are managed and have valid IDs.
        Object ownerIdVal = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
        Object relatedIdVal = IdentityUtils.getTargetKeyForDatastoreIdentity(relatedSM.getInternalObjectId());

        String ownerLabel = sm.getClassMetaData().getName();
        String relatedLabel = relatedSM.getClassMetaData().getName();
        String relType = mmd.getName().toUpperCase();

        // MERGE is used to prevent creating duplicate relationships
        String cypher = String.format("MATCH (a:%s), (b:%s) WHERE id(a) = $ownerId AND id(b) = $relatedId MERGE (a)-[:%s]->(b)", 
            ownerLabel, relatedLabel, relType);
        
        Map<String, Object> params = new HashMap<>();
        params.put("ownerId", ownerIdVal);
        params.put("relatedId", relatedIdVal);
        tx.run(cypher, params);
    }

    public void deleteRelationField(AbstractMemberMetaData mmd, Object relatedObject) {
        if (relatedObject == null) {
            return;
        }
        if (relatedObject instanceof Collection) {
            for (Object element : (Collection<?>) relatedObject) {
                deleteRelationship(mmd, element);
            }
        } else {
            deleteRelationship(mmd, relatedObject);
        }
    }

    private void deleteRelationship(AbstractMemberMetaData mmd, Object relatedObject) {
        // This method handles cascade-delete for dependent fields.
        if (mmd.isDependent()) {
            DNStateManager relatedSM = sm.getExecutionContext().findStateManager(relatedObject);
            if (relatedSM == null || relatedSM.getInternalObjectId() == null) {
                return;
            }

            Object relatedIdVal = IdentityUtils.getTargetKeyForDatastoreIdentity(relatedSM.getInternalObjectId());
            String relatedLabel = relatedSM.getClassMetaData().getName();

            // DETACH DELETE will remove the node and all of its relationships.
            String cypherNodeDelete = String.format("MATCH (b:%s) WHERE id(b) = $relatedId DETACH DELETE b", relatedLabel);
            Map<String, Object> nodeDeleteParams = new HashMap<>();
            nodeDeleteParams.put("relatedId", relatedIdVal);

            tx.run(cypherNodeDelete, nodeDeleteParams);
        }
    }
}