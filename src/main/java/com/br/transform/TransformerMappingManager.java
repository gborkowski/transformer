package com.br.transform;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransformerMappingManager {
    private static final Logger LOG = LogManager.getLogger(TransformerMappingManager.class);

    private String customerName = "";

    private List<FieldMap> headers =  new ArrayList<FieldMap>();

    private Firestore firestoreDB;

    public String getCustomerName() {
        return this.customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public List<FieldMap> getHeaders() {
        return this.headers;
    }

    public void setHeaders(List<FieldMap> headers) {
        this.headers = headers;
    }

    public Firestore getFirestoreDB() {
        return firestoreDB;
    }

    public void setFirestoreDB(Firestore firestoreDB) {
        this.firestoreDB = firestoreDB;
    }

    /***********************************************/

    public void writeFieldMapToDB() {
        try {
            Firestore db = getFirestoreDB();
            CollectionReference customerProperties = db.collection("customerFieldMaps");
            Query customerQuery = customerProperties.whereEqualTo("customerName", getCustomerName());

            // asynchronously retrieve the fieldMap if it exists
            ApiFuture<QuerySnapshot> query = customerQuery.get();

            // query.get() blocks on response
            QuerySnapshot querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

            if (documents.size() == 0) {
                // no fieldMaps document
                LOG.info("writeFieldMapToDB: No document for fieldMaps yet - creating");

                // create new document with auto-gen ID
                DocumentReference newDoc = db.collection("customerFieldMaps").document();
                LOG.info("writeFieldMapToDB: Created docId: " + newDoc.getId());

                // setup data
                Map<String, Object> docData = new HashMap<>();
                docData.put("customerName", getCustomerName());
                docData.put("fieldMaps", getHeaders());

                // write to DB
                newDoc.set(docData);
                LOG.info("writeFieldMapToDB: updated DB (result)");

            } else {
                String docId = "";
                for (QueryDocumentSnapshot document : documents) {
                    docId = document.getId();
                }
                LOG.info("writeFieldMapToDB: found docId (not updating): " + docId);
            }

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error writing mapping info to DB for customer: " + getCustomerName());
        }
    }

    /***********************************************/

    public void writeRowMapToDB() {
        try {
            Firestore db = getFirestoreDB();
            CollectionReference customerProperties = db.collection("customerRowMaps");
            Query customerQuery = customerProperties.whereEqualTo("customerName", getCustomerName());

            // asynchronously retrieve the fieldMap if it ex`ists
            ApiFuture<QuerySnapshot> query = customerQuery.get();

            // query.get() blocks on response
            QuerySnapshot querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

            if (documents.size() == 0) {
                // no fieldMaps document
                LOG.info("writeRowMapToDB: No document for rowMaps yet - creating");

                // create new document with auto-gen ID
                DocumentReference newDoc = db.collection("customerRowMaps").document();
                LOG.info("writeRowMapToDB: Created docId: " + newDoc.getId());

                // setup data
                Map<String, Object> docData = new HashMap<>();
                docData.put("customerName", getCustomerName());
                docData.put("filterProcessors", new ArrayList<RowFilterConfig>());
                docData.put("splitProcessors", new ArrayList<RowSplitConfig>());
                docData.put("mergeProcessors", new ArrayList<RowMergeConfig>());

                // write to DB
                newDoc.set(docData);
                LOG.info("writeRowMapToDB: updated DB (result)");

            } else {
                String docId = "";
                for (QueryDocumentSnapshot document : documents) {
                    docId = document.getId();
                }
                LOG.info("writeRowMapToDB: found docId - not writing anything: " + docId);
            }

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error writing mapping info to DB for customer: " + getCustomerName());
        }
    }

    /***********************************************/

    public SuperRowMap getRowMapFromDB() {

        try {
            Firestore db = getFirestoreDB();
            CollectionReference customerRM = db.collection("customerRowMaps");
            Query customerQuery = customerRM.whereEqualTo("customerName", getCustomerName());

            // asynchronously retrieve the rowMap if it exists
            ApiFuture<QuerySnapshot> query = customerQuery.get();

            // query.get() blocks on response
            QuerySnapshot querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

            if (documents.size() > 0) {
                String docId = "";
                for (QueryDocumentSnapshot document : documents) {
                    docId = document.getId();
                }
                LOG.info("getRowMapFromDB: found docId: " + docId);
                DocumentReference existingDoc = db.collection("customerRowMaps").document(docId);

                // get existing data
                ApiFuture<DocumentSnapshot> future = existingDoc.get();
                DocumentSnapshot currentDoc = future.get();
                SuperRowMap srm = currentDoc.toObject(SuperRowMap.class); // null
                return srm;
            } else {
                LOG.info("getRowMapFromDB: No row map found.");
            }

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("getRowMapFromDB: Error getting row map: " + getCustomerName());
        }
        return null;
    }
    /***********************************************/

    public SuperFieldMap getFieldMapFromDB() {

        try {
            Firestore db = getFirestoreDB();
            CollectionReference customerFM = db.collection("customerFieldMaps");
            Query customerQuery = customerFM.whereEqualTo("customerName", getCustomerName());

            // asynchronously retrieve the fieldMap if it exists
            ApiFuture<QuerySnapshot> query = customerQuery.get();

            // query.get() blocks on response
            QuerySnapshot querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

            if (documents.size() > 0) {
                String docId = "";
                for (QueryDocumentSnapshot document : documents) {
                    docId = document.getId();
                }
                LOG.info("getFieldMapFromDB: found docId: " + docId);
                DocumentReference existingDoc = db.collection("customerFieldMaps").document(docId);

                // get existing data
                ApiFuture<DocumentSnapshot> future = existingDoc.get();
                DocumentSnapshot currentDoc = future.get();
                SuperFieldMap sfm = currentDoc.toObject(SuperFieldMap.class);
                return sfm;
            } else {
                LOG.info("getFieldMapFromDB: No field map found.");
            }

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("getFieldMapFromDB: Error getting field map: " + getCustomerName());
        }
        return null;
    }
}
