package com.br.transform;

import com.google.cloud.firestore.Firestore;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ImportPhase.
 * Main class for import phase
 *
 */

public class ImportPhase {
    private static final Logger LOG = LogManager.getLogger(ImportPhase.class);

    /* Firestore DB */
    private Firestore firestoreDB;

    /* stores headers for import file */
    private ImportHeaderInfo ihi = new ImportHeaderInfo();

    /* just an empty object - to write out a mostly blank file */
    private RowMap rowMap = new RowMap();

    public RowMap getRowMap() {
        return rowMap;
    }

    public Firestore getFirestoreDB() {
        return firestoreDB;
    }

    public void setFirestoreDB(Firestore firestoreDB) {
        this.firestoreDB = firestoreDB;
    }
    public static void main(String[] args) {

        if (args.length == 0) {
            LOG.info("Error: you must specify the sub directory of the properties files (i.e. customer)");
            System.exit(0);
        }

        long startTotal = System.currentTimeMillis();

        ImportPhase importPhase = new ImportPhase();

        /* Getting properties file */
        TransformerPropertiesManager tpm = new TransformerPropertiesManager();
        tpm.setCustomerName(args[0]);
        TransformerProperties tp = tpm.getTransformerProperties();
        importPhase.setFirestoreDB(tpm.getFirestoreDB());

        /* Individual properties */
        String cMainCustomerDirectory = tp.getMainCustomerDirectory();
        String cTransformOutputDirectory = tp.getTransformOutputDirectory();

        String cInputFile = tp.getImportInputFile();
        String cFileType = tp.getFileType();
        String cOutputFile = tp.getImportOutputFile();
        String cCharacterSet = tp.getCharacterSet();
        Boolean cHasVariants = tp.getHasVariants();

        /* Delimited properties */
        String cFieldSeparator = tp.getFieldSeparator();
        Boolean cHeaderRowExists = tp.getHeaderRowExists();
        boolean headerRowExists = cHeaderRowExists.booleanValue();

        /* XML properties */
        String cMainXMLEntity = tp.getMainXMLEntity();
        String cProductXMLEntity = tp.getProductXMLEntity();
        String cVariantXMLEntity = tp.getVariantXMLEntity();
        String cPreserveValuesForSourceEntities = tp.getPreserveValuesForSourceEntities();

        /* Check that files & directories are there */
        importPhase.createTransformOutputDirectoryIfMissing(cMainCustomerDirectory + cTransformOutputDirectory);

        /* setup file variables */
        cInputFile = cMainCustomerDirectory + cInputFile;
        cOutputFile = cMainCustomerDirectory + cTransformOutputDirectory + cOutputFile;

        // Process based on file type
        if ("delimited".equals(cFileType)) {
            ImportParseDelimited ipd = new ImportParseDelimited();
            ipd.processDelimited(
                cFileType,
                cInputFile,
                cFieldSeparator,
                headerRowExists,
                cOutputFile,
                cCharacterSet,
                importPhase.ihi);
        } else if ("XML".equals(cFileType)) {
            // handle XML
            ImportParseXML ipx = new ImportParseXML();
            ipx.processXML(cFileType,
                cInputFile,
                cOutputFile,
                cCharacterSet,
                cMainXMLEntity,
                cProductXMLEntity,
                cVariantXMLEntity,
                cHasVariants.booleanValue(),
                cPreserveValuesForSourceEntities,
                importPhase.ihi);
        } else if ("JSON".equals(cFileType)) {
            ImportParseJson ipj = new ImportParseJson();
            ipj.processJson(cFileType, cInputFile, cOutputFile, cCharacterSet, importPhase.ihi);
        }

        // write out fieldMap and rowMap config JSON files
        importPhase.writeMapsToDB(args[0]);

        LOG.info("Total Time Taken : " + (System.currentTimeMillis() - startTotal) / 1000 + " secs");
    }

    /***********************************************/

    public void writeMapsToDB(String customerName) {

        TransformerMappingManager tmm = new TransformerMappingManager();
        tmm.setCustomerName(customerName);
        tmm.setHeaders(ihi.getHeaders());

        // get firestoreDB from earlier
        tmm.setFirestoreDB(getFirestoreDB());
        tmm.writeFieldMapToDB();
        tmm.writeRowMapToDB();
    }

    /***********************************************/

    public void createTransformOutputDirectoryIfMissing(String dirname) {
        File dir = new File(dirname);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }
}
