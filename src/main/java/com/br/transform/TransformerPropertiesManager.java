package com.br.transform;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransformerPropertiesManager {
    private static final Logger LOG = LogManager.getLogger(TransformerPropertiesManager.class);

    private String customerName = "";

    public String getCustomerName() {
        return this.customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public TransformerProperties getTransformerProperties() {
        //return getPropertiesFromFile();
        return getPropertiesFromDB();
    }
    /***********************************************/
    /*
    private TransformerProperties getPropertiesFromFile() {
        //import java.io.FileInputStream;
        //import java.util.Properties;

        TransformerProperties tp = new TransformerProperties();
        try {
            // Getting properties
            String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath() + "properties/";
            String appConfigPath = rootPath + getCustomerName() + "/" + "transform.properties";
            Properties appProps = new Properties();
            appProps.load(new FileInputStream(appConfigPath));

            // Individual properties
            tp.setCustomerName(appProps.getProperty("customerName"));
            tp.setHasProductAttributes(Boolean.parseBoolean(appProps.getProperty("hasProductAttributes")));
            tp.setHasVariants(Boolean.parseBoolean(appProps.getProperty("hasVariants")));
            tp.setHasVariantAttributes(Boolean.parseBoolean(appProps.getProperty("hasVariantAttributes")));
            tp.setOutputFormat(appProps.getProperty("outputFormat"));
            tp.setMainCustomerDirectory(appProps.getProperty("mainCustomerDirectory"));
            tp.setTransformOutputDirectory(appProps.getProperty("transformOutputDirectory"));
            tp.setFieldMapFile(appProps.getProperty("fieldMapFile"));
            tp.setRowMapFile(appProps.getProperty("rowMapFile"));
            tp.setImportInputFile(appProps.getProperty("importInputFile"));
            tp.setImportOutputFile(appProps.getProperty("importOutputFile"));
            tp.setCharacterSet(appProps.getProperty("characterSet"));
            tp.setHeaderRowExists(Boolean.parseBoolean(appProps.getProperty("headerRowExists")));
            tp.setFileType(appProps.getProperty("fileType"));
            tp.setMainXMLEntity(appProps.getProperty("mainXMLEntity"));
            tp.setProductXMLEntity(appProps.getProperty("productXMLEntity"));
            tp.setVariantXMLEntity(appProps.getProperty("variantXMLEntity"));
            tp.setFieldSeparator(appProps.getProperty("fieldSeparator"));
            tp.setTransformOutputProductFile(appProps.getProperty("transformOutputProductFile"));
            tp.setTransformOutputVariantFile(appProps.getProperty("transformOutputVariantFile"));
            tp.setOutputOutputFile(appProps.getProperty("outputOutputFile"));
            tp.setPreserveValuesForSourceEntities(appProps.getProperty("preserveValuesForSourceEntities"));

        } catch (IOException e) {
            LOG.error("Error loading properties from file for customer: " + getCustomerName());
        }

        return tp;
    }
    */
    /***********************************************/

    private TransformerProperties getPropertiesFromDB() {

        TransformerProperties tp = new TransformerProperties();
        try {
            /* Getting properties */
            FileInputStream serviceAccount = new FileInputStream("transform-2a333-firebase-adminsdk-26yti-b42ad15736.json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);

            FirebaseOptions options = FirebaseOptions.builder()
                .setCredentials(credentials)
                .setProjectId("transform-2a333")
                .build();
            FirebaseApp.initializeApp(options);

            Firestore db = FirestoreClient.getFirestore();
            CollectionReference customerProperties = db.collection("customerProperties");
            Query customerQuery = customerProperties.whereEqualTo("customerName", getCustomerName());

            // asynchronously retrieve all users
            ApiFuture<QuerySnapshot> query = customerQuery.get();

            // query.get() blocks on response
            QuerySnapshot querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

            for (QueryDocumentSnapshot document : documents) {
                /* Individual properties */
                tp.setCustomerName(document.getString("customerName"));
                tp.setHasProductAttributes(Boolean.parseBoolean(document.getString("hasProductAttributes")));
                tp.setHasVariants(Boolean.parseBoolean(document.getString("hasVariants")));
                tp.setHasVariantAttributes(Boolean.parseBoolean(document.getString("hasVariantAttributes")));
                tp.setOutputFormat(document.getString("outputFormat"));
                tp.setMainCustomerDirectory(document.getString("mainCustomerDirectory"));
                tp.setTransformOutputDirectory(document.getString("transformOutputDirectory"));
                tp.setFieldMapFile(document.getString("fieldMapFile"));
                tp.setRowMapFile(document.getString("rowMapFile"));
                tp.setImportInputFile(document.getString("importInputFile"));
                tp.setImportOutputFile(document.getString("importOutputFile"));
                tp.setCharacterSet(document.getString("characterSet"));
                tp.setHeaderRowExists(Boolean.parseBoolean(document.getString("headerRowExists")));
                tp.setFileType(document.getString("fileType"));
                tp.setMainXMLEntity(document.getString("mainXMLEntity"));
                tp.setProductXMLEntity(document.getString("productXMLEntity"));
                tp.setVariantXMLEntity(document.getString("variantXMLEntity"));
                tp.setFieldSeparator(document.getString("fieldSeparator"));
                tp.setTransformOutputProductFile(document.getString("transformOutputProductFile"));
                tp.setTransformOutputVariantFile(document.getString("transformOutputVariantFile"));
                tp.setOutputOutputFile(document.getString("outputOutputFile"));
                tp.setPreserveValuesForSourceEntities(document.getString("preserveValuesForSourceEntities"));
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Error loading properties from DB for customer: " + getCustomerName());
        }

        return tp;
    }

}
