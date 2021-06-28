package com.br.transform;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OutputPhase {
    private static final Logger LOG = LogManager.getLogger(OutputPhase.class);

    public static void main(String[] args) {

        if (args.length == 0) {
            LOG.error("Error: you must specify the sub directory of the properties files (i.e. customer)");
            System.exit(0);
        }

        OutputPhase op = new OutputPhase();

        // this is the main output file (used for indexing)
        long startTotal = System.currentTimeMillis();

        try {
            /* Getting properties file */
            String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath() + "properties/";
            String appConfigPath = rootPath + args[0] + "/" + "transform.properties";
            Properties appProps = new Properties();
            appProps.load(new FileInputStream(appConfigPath));

            /* Individual properties */
            String cMainCustomerDirectory = appProps.getProperty("mainCustomerDirectory");
            String cTransformOutputDirectory = appProps.getProperty("transformOutputDirectory");
            String cInputProductFile = appProps.getProperty("transformOutputProductFile");
            String cInputVariantFile = appProps.getProperty("transformOutputVariantFile");
            String cHasVariants = appProps.getProperty("hasVariants");
            boolean hasVariants = Boolean.parseBoolean(cHasVariants);
            String cOutputFile = appProps.getProperty("outputOutputFile");
            String cOutputFormat = appProps.getProperty("outputFormat");

            /* setup file variables */
            cInputProductFile = cMainCustomerDirectory + cTransformOutputDirectory + cInputProductFile;
            cInputVariantFile = cMainCustomerDirectory + cTransformOutputDirectory + cInputVariantFile;
            cOutputFile = cMainCustomerDirectory + cTransformOutputDirectory + cOutputFile;

            // delegate work
            op.delegate(cOutputFormat, cInputProductFile, cInputVariantFile, hasVariants, cOutputFile);

        } catch (IOException e) {
            LOG.error("OutputPhase: " + e);
        }

        LOG.info("Total Time Taken : " + (System.currentTimeMillis() - startTotal) / 1000 + " secs");
    }

    /***********************************************/

    public void delegate(String cOutputFormat, String cInputProductFile, String cInputVariantFile, boolean hasVariants, String cOutputFile) {
        // Process based on file type
        if ("connect".equals(cOutputFormat)) {
            OutputConnect oc = new OutputConnect();
            oc.output(cInputProductFile, cInputVariantFile, hasVariants, cOutputFile);
        } else if ("dataConnect".equals(cOutputFormat)) {
            OutputDataConnect odc = new OutputDataConnect();
            odc.output(cInputProductFile, cInputVariantFile, hasVariants, cOutputFile);
        } else {
            LOG.error("Unsupported outputFormat (properties file).");
        }
    }
}
