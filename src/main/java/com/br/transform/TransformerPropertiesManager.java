package com.br.transform;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
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
        return getPropertiesFromFile();
    }

    /***********************************************/

    private TransformerProperties getPropertiesFromFile() {

        TransformerProperties tp = new TransformerProperties();
        try {
            /* Getting properties */
            String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath() + "properties/";
            String appConfigPath = rootPath + getCustomerName() + "/" + "transform.properties";
            Properties appProps = new Properties();
            appProps.load(new FileInputStream(appConfigPath));

            /* Individual properties */
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

    /***********************************************/

    /*
    private TransformerProperties getPropertiesFromFirestoreDB(TransformerProperties tp) {
        TransformerProperties tp = new TransformerProperties();
        try {

            // get from DB

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
}
