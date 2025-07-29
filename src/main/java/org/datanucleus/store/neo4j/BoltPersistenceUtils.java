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

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.DNStateManager;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;

/**
 * Utility methods for persistence operations using the Bolt driver.
 * This class encapsulates the logic for retrieving a Neo4j Node,
 * prioritizing the in-transaction cache before querying the database.
 */
public class BoltPersistenceUtils {

    public static Node getPropertyContainerForStateManager(Transaction tx, DNStateManager sm) {
        Object associatedValue = sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
        if (associatedValue instanceof Node) {
            return (Node) associatedValue;
        }

        Object id = sm.getInternalObjectId();
        if (id == null) {
            return null;
        }
        
        if (sm.getClassMetaData().getIdentityType() == IdentityType.APPLICATION) {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            String[] pkMemberNames = cmd.getPrimaryKeyMemberNames();
            String pkMemberName = pkMemberNames[0]; // Assuming single field PK for simplicity

            String cypher = String.format("MATCH (n:`%s` {`%s`: $pkValue}) RETURN n", cmd.getName(), pkMemberName);
            Object pkValue = IdentityUtils.getTargetKeyForSingleFieldIdentity(id);
            
            Result result = tx.run(cypher, Values.parameters("pkValue", pkValue));
            return result.hasNext() ? result.single().get("n").asNode() : null;
        }

        Long nodeId = (Long) IdentityUtils.getTargetKeyForDatastoreIdentity(id);
        if (nodeId == null) {
            return null;
        }

        Result result = tx.run("MATCH (n) WHERE id(n) = $id RETURN n", Values.parameters("id", nodeId));
        return result.hasNext() ? result.single().get("n").asNode() : null;
    }

    /**
     * Finds or creates a managed Java object for a given Neo4j Node.
     * It constructs the object's identity and asks the ExecutionContext to find it,
     * creating a hollow object if it's not already managed.
     * @param ec The ExecutionContext.
     * @param node The Neo4j Node.
     * @param cmd The metadata for the class of the Java object.
     * @return The managed Java object.
     */
    public static Object getObjectForNode(ExecutionContext ec, Node node, AbstractClassMetaData cmd) {
        Object id;
        if (cmd.getIdentityType() == IdentityType.DATASTORE) {
            id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), node.id());
        } 
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // ============================ THE DEFINITIVE FIX ============================
            Class pcType = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
            int[] pkMemberPositions = cmd.getPKMemberPositions();
            if (pkMemberPositions.length == 1) {
                // Handle single-field application identity
                String pkMemberName = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkMemberPositions[0]).getName();
                Object pkValue = node.get(pkMemberName).asObject();
                id = ec.getNucleusContext().getIdentityManager().getApplicationId(pcType, pkValue);
            } else {
                // Handle composite (multi-field) application identity
                StringBuilder pkStr = new StringBuilder();
                for (int i = 0; i < pkMemberPositions.length; i++) {
                    String pkMemberName = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkMemberPositions[i]).getName();
                    Object pkValue = node.get(pkMemberName).asObject();
                    if (i > 0) {
                        pkStr.append('_');
                    }
                    pkStr.append(pkValue.toString());
                }
                // Use the correct API that takes a Class and a key Object (here, a String representation)
                id = ec.getNucleusContext().getIdentityManager().getApplicationId(pcType, pkStr.toString());
            }
            // ========================== END OF THE DEFINITIVE FIX =======================
        }
        else 
        {
            throw new NucleusDataStoreException("IdentityType " + cmd.getIdentityType() + " not supported");
        }

        return ec.findObject(id, true, true, null);
    }
}