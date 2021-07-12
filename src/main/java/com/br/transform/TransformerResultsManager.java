package com.br.transform;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransformerResultsManager {
    private static final Logger LOG = LogManager.getLogger(TransformerResultsManager.class);

    private String customerName = "";

    private Firestore firestoreDB;

    public String getCustomerName() {
        return this.customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public Firestore getFirestoreDB() {
        return firestoreDB;
    }

    public void setFirestoreDB(Firestore firestoreDB) {
        this.firestoreDB = firestoreDB;
    }

    /***********************************************/

    public void writeResultsToDB(SuperResults sr) {

        try {
            Firestore db = getFirestoreDB();
            CollectionReference customerResults = db.collection("customerResults");
            Query customerQuery = customerResults.whereEqualTo("customerName", getCustomerName());

            // asynchronously retrieve the fieldMap if it ex`ists
            ApiFuture<QuerySnapshot> query = customerQuery.get();

            // query.get() blocks on response
            QuerySnapshot querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

            if (documents.size() == 0) {
                // no results document for this customer
                LOG.info("writeResultsToDB: No document for results yet - creating");

                // create new document with auto-gen ID
                DocumentReference newDoc = db.collection("customerResults").document();
                LOG.info("writeResultsToDB: Created docId: " + newDoc.getId());

                // setup data
                Map<String, Object> docData = new HashMap<>();
                docData.put("customerName", sr.getCustomerName());
                docData.put("totalProducts", sr.getTotalProducts());
                docData.put("totalVariants", sr.getTotalVariants());
                docData.put("totalUniqueProductAttributes", sr.getTotalUniqueProductAttributes());
                docData.put("totalUniqueVariantAttributes", sr.getTotalUniqueVariantAttributes());
                docData.put("top50ProductAttributes", sr.getTop50ProductAttributes());
                docData.put("top50VariantAttributes", sr.getTop50VariantAttributes());

                // write to DB
                newDoc.set(docData);
                LOG.info("writeResultsToDB: updated DB (result)");
            } else {
                String docId = "";
                for (QueryDocumentSnapshot document : documents) {
                    docId = document.getId();
                }
                LOG.info("writeResultsToDB: found docId: " + docId);
                DocumentReference existingDoc = db.collection("customerResults").document(docId);

                // setup data
                Map<String, Object> docData = new HashMap<>();
                docData.put("customerName", sr.getCustomerName());
                docData.put("totalProducts", sr.getTotalProducts());
                docData.put("totalVariants", sr.getTotalVariants());
                docData.put("totalUniqueProductAttributes", sr.getTotalUniqueProductAttributes());
                docData.put("totalUniqueVariantAttributes", sr.getTotalUniqueVariantAttributes());
                docData.put("top50ProductAttributes", sr.getTop50ProductAttributes());
                docData.put("top50VariantAttributes", sr.getTop50VariantAttributes());

                // write to DB
                existingDoc.set(docData);
                LOG.info("writeResultsToDB: updated DB (result)");
            }

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error writing mapping info to DB for customer: " + getCustomerName());
        }
    }

}
