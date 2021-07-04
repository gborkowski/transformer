package com.br.transform;

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

        /* Getting properties file */
        TransformerPropertiesManager tpm = new TransformerPropertiesManager();
        tpm.setCustomerName(args[0]);
        TransformerProperties tp = tpm.getTransformerProperties();

        /* Individual properties */
        String cMainCustomerDirectory = tp.getMainCustomerDirectory();
        String cTransformOutputDirectory = tp.getTransformOutputDirectory();
        String cInputProductFile = tp.getTransformOutputProductFile();
        String cInputVariantFile = tp.getTransformOutputVariantFile();
        Boolean cHasVariants = tp.getHasVariants();
        String cOutputFile = tp.getOutputOutputFile();
        String cOutputFormat = tp.getOutputFormat();

        /* setup file variables */
        cInputProductFile = cMainCustomerDirectory + cTransformOutputDirectory + cInputProductFile;
        cInputVariantFile = cMainCustomerDirectory + cTransformOutputDirectory + cInputVariantFile;
        cOutputFile = cMainCustomerDirectory + cTransformOutputDirectory + cOutputFile;

        // delegate work
        op.delegate(cOutputFormat, cInputProductFile, cInputVariantFile, cHasVariants.booleanValue(), cOutputFile);

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
