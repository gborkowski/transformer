package com.br.transform;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ImportPhase.
 * Main class for import phase
 *
 */

public class ImportPhase {
    private static final Logger LOG = LogManager.getLogger(ImportPhase.class);

    /* stores headers for import file */
    private ImportHeaderInfo ihi = new ImportHeaderInfo();

    /* just an empty object - to write out a mostly blank file */
    private RowMap rowMap = new RowMap();

    public RowMap getRowMap() {
        return rowMap;
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

        /* Individual properties */
        String cMainCustomerDirectory = tp.getMainCustomerDirectory();
        String cTransformOutputDirectory = tp.getTransformOutputDirectory();

        String cInputFile = tp.getImportInputFile();
        String cFileType = tp.getFileType();
        String cOutputFile = tp.getImportOutputFile();
        String cRowMapFile = tp.getRowMapFile();
        String cFieldMapFile = tp.getFieldMapFile();
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
        cFieldMapFile = cMainCustomerDirectory + cTransformOutputDirectory + cFieldMapFile;
        cRowMapFile = cMainCustomerDirectory + cTransformOutputDirectory + cRowMapFile;

        /* Check fieldMap file */
        boolean okToWrite = importPhase.checkFieldMapFile(cFieldMapFile);
        if (!okToWrite) {
            System.out.println();
            LOG.info("FieldMap file has been changed - don't want to overwrite any of your changes.");
            LOG.info("You need to either remove or rename the configured fieldMap file: ");
            System.out.println(cFieldMapFile);
            System.out.println();
            System.exit(0);
        }

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
        importPhase.writeMapFiles(cFieldMapFile, cRowMapFile);

        LOG.info("Total Time Taken : " + (System.currentTimeMillis() - startTotal) / 1000 + " secs");
    }

    /***********************************************/

    public void writeMapFiles(String fieldMapFilename, String rowMapFilename) {
        try (Writer writer = new FileWriter(fieldMapFilename)) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(ihi.getHeaders(), writer);
            writer.close();
        } catch (IOException e) {
            LOG.error("writeMapFiles(fieldMap): " + e);
        }

        try (Writer writer = new FileWriter(rowMapFilename)) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(getRowMap(), writer);
            writer.close();
        } catch (IOException e) {
            LOG.error("writeMapFiles(rowMap): " + e);
        }
    }

    /***********************************************/

    public void createTransformOutputDirectoryIfMissing(String dirname) {
        File dir = new File(dirname);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }

    /***********************************************/

    public boolean checkFieldMapFile(String filename) {
        boolean ok = true;
        FileTime createTime;
        FileTime modifyTime;

        try {
            // check if file exists first
            File fieldMapFile = new File(filename);
            if (fieldMapFile.exists()) {
                Path path = Paths.get(filename);
                BasicFileAttributeView basicView = Files.getFileAttributeView(path, BasicFileAttributeView.class);
                BasicFileAttributes basicAttribs = basicView.readAttributes();
                createTime = basicAttribs.creationTime();
                modifyTime = basicAttribs.lastModifiedTime();

                if (createTime.compareTo(modifyTime) < 0) {
                    ok = false;
                }
            }
        } catch (IOException e) {
            LOG.error("checkFieldMapFile: Cannot get the last modified time.");
            ok = false;
        }
        return ok;
    }
}
