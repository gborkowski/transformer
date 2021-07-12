package com.br.transform;

import com.google.cloud.firestore.Firestore;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
    Analysis (additional)
*/

public class TransformPhase {
    private static final Logger LOG = LogManager.getLogger(TransformPhase.class);

    /* Firestore DB */
    private Firestore firestoreDB;

    /* some settings to store globally */
    private boolean configuredProductAttributes;

    private boolean configuredVariantAttributes;

    private Attributes attributes = new Attributes();

    /* RowMap config object */
    private RowMap rowMap = new RowMap();

    /* FieldMap config object */
    private List<FieldMap> fieldMap = new ArrayList<FieldMap>();

    /* temporary lists used to perform validation */
    private List<String> pidList = new ArrayList<String>();

    private List<JsonObject> productList = new ArrayList<JsonObject>();

    private List<String> skuIdList = new ArrayList<String>();

    private List<JsonObject> variantList = new ArrayList<JsonObject>();

    public boolean getConfiguredProductAttributes() {
        return configuredProductAttributes;
    }

    public void setConfiguredProductAttributes(boolean configuredProductAttributes) {
        this.configuredProductAttributes = configuredProductAttributes;
    }

    public boolean getConfiguredVariantAttributes() {
        return configuredVariantAttributes;
    }

    public void setConfiguredVariantAttributes(boolean configuredVariantAttributes) {
        this.configuredVariantAttributes = configuredVariantAttributes;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public RowMap getRowMap() {
        return rowMap;
    }

    public void setRowMap(RowMap rowMap) {
        this.rowMap = rowMap;
    }

    public List<FieldMap> getFieldMap() {
        return fieldMap;
    }

    public void setFieldMap(List<FieldMap> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public Firestore getFirestoreDB() {
        return firestoreDB;
    }

    public void setFirestoreDB(Firestore firestoreDB) {
        this.firestoreDB = firestoreDB;
    }
    public static void main(String[] args) {

        if (args.length == 0) {
            LOG.error("Error: you must specify the customer name, exiting...");
            System.exit(0);
        }

        long startTotal = System.currentTimeMillis();

        TransformPhase transformPhase = new TransformPhase();

        /* Getting properties */
        TransformerPropertiesManager tpm = new TransformerPropertiesManager();
        tpm.setCustomerName(args[0]);
        TransformerProperties tp = tpm.getTransformerProperties();
        transformPhase.setFirestoreDB(tpm.getFirestoreDB());

        /* Getting Row and Field maps */
        TransformerMappingManager tmm = new TransformerMappingManager();
        tmm.setCustomerName(args[0]);
        tmm.setFirestoreDB(transformPhase.getFirestoreDB());
        RowMap rm = new RowMap();
        SuperRowMap srm = tmm.getRowMapFromDB();
        if (srm != null) {
            rm.setFilterProcessors(srm.getFilterProcessors());
            rm.setSplitProcessors(srm.getSplitProcessors());
            rm.setMergeProcessors(srm.getMergeProcessors());
            transformPhase.setRowMap(rm);
        } else {
            LOG.error("Error: Could not load row maps from DB.  srm is null, exiting...");
            System.exit(0);
        }
        SuperFieldMap sfm = tmm.getFieldMapFromDB();
        if (sfm != null) {
            transformPhase.setFieldMap(sfm.getFieldMaps());
        } else {
            LOG.error("Error: Could not load field maps from DB.  sfm is null, exiting...");
            System.exit(0);
        }

        /* Individual properties */
        String cMainCustomerDirectory = tp.getMainCustomerDirectory();
        String cTransformOutputDirectory = tp.getTransformOutputDirectory();
        String cCustomerName = tp.getCustomerName();
        Boolean cHasVariants = tp.getHasVariants();
        Boolean cHasProductAttributes = tp.getHasProductAttributes();
        Boolean cHasVariantAttributes = tp.getHasVariantAttributes();
        String cOutputFormat = tp.getOutputFormat();
        String cInputFile = tp.getImportOutputFile();
        String cOutputProductFile = tp.getTransformOutputProductFile();
        String cOutputVariantFile = tp.getTransformOutputVariantFile();

        /* gloabl settings */
        transformPhase.setConfiguredProductAttributes(cHasProductAttributes);
        transformPhase.setConfiguredVariantAttributes(cHasVariantAttributes);

        /* setup file variables */
        cInputFile = cMainCustomerDirectory + cTransformOutputDirectory + cInputFile;
        cOutputProductFile = cMainCustomerDirectory + cTransformOutputDirectory + cOutputProductFile;
        cOutputVariantFile = cMainCustomerDirectory + cTransformOutputDirectory + cOutputVariantFile;
        String analysisDirectory = cMainCustomerDirectory + cTransformOutputDirectory;

        /* validate fieldMap attribute config */
        boolean configValid = transformPhase.validateFieldMapAttributeConfig(cHasVariants.booleanValue());
        if (!configValid) {
            LOG.error("Config validation failed, exiting.");
            System.exit(0);
        }

        /* validate fieldMap config for some stuff */
        System.out.println();
        boolean valid = transformPhase.validateFieldMapConfig(cOutputFormat, cHasVariants.booleanValue());
        System.out.println();
        if (valid) {
            /* process rows */
            transformPhase.processJson(cCustomerName, cInputFile, cOutputProductFile,
                cOutputVariantFile, cHasVariants.booleanValue());
        }

        /* perform analysis on products / variants --> output file */
        transformPhase.performAnalysis(cCustomerName, cHasVariants.booleanValue(), analysisDirectory);

        LOG.info("Total Time Taken : " + (System.currentTimeMillis() - startTotal) / 1000 + " secs");
    }

    /***********************************************/

    public boolean validateFieldMapAttributeConfig(boolean hasVariants) {

        List<FieldMap> fm = getFieldMap();
        for (FieldMap item : fm) {
            String targetLabel = item.getTargetLabel();
            String targetEntity = item.getTargetEntity();
            Boolean isAttribute = item.getIsAttribute();
            Boolean ignoreThisField = item.getIgnoreThisField();

            if (!ignoreThisField.booleanValue() && !hasVariants && "variant".equals(targetEntity)) {
                LOG.error("validateFieldMapAttributeConfig: hasVariants is false, but this is marked as variant targetEntity: " + targetLabel);
                return false;
            }
            if (!ignoreThisField.booleanValue() && !getConfiguredProductAttributes()
                && "product".equals(targetEntity) && isAttribute.booleanValue()) {
                LOG.error("validateFieldMapAttributeConfig: hasProductAttributes is false, but this is marked as product targetEntity with isAttribute=true: " + targetLabel);
                return false;
            }
            if (!ignoreThisField.booleanValue() && !getConfiguredVariantAttributes()
                && "variant".equals(targetEntity) && isAttribute.booleanValue()) {
                LOG.error("validateFieldMapAttributeConfig: hasVariantAttributes is false, but this is marked as variant targetEntity with isAttribute=true: " + targetLabel);
                return false;
            }
        }

        return true;
    }

    /***********************************************/

    public boolean validateFieldMapConfig(String outputFormat, boolean hasVariants) {
        List<FieldMap> fm = getFieldMap();
        boolean hasPid = false;
        boolean hasTitle = false;
        boolean hasDescription = false;
        boolean hasUrl = false;
        boolean hasAvailability = false;
        boolean hasCrumbs = false;
        boolean hasCrumbs_id = false;
        boolean hasPrice = false;
        boolean hasThumb_image = false;
        boolean hasCategory_paths = false;
        boolean hasBrand = false;
        boolean containsVariants = false;
        // add others as required

        for (FieldMap item : fm) {
            String targetLabel = item.getTargetLabel();

            // validate label
            if (!validateLabel(targetLabel)) {
                return false;
            }

            switch (targetLabel.toUpperCase()) {
                case "PID":
                    hasPid = true;
                    break;
                case "TITLE":
                    hasTitle = true;
                    break;
                case "DESCRIPTION":
                    hasDescription = true;
                    break;
                case "URL":
                    hasUrl = true;
                    break;
                case "AVAILABILITY":
                    hasAvailability = true;
                    break;
                case "CRUMBS":
                    hasCrumbs = true;
                    break;
                case "CRUMBS_ID":
                    hasCrumbs_id = true;
                    break;
                case "PRICE":
                    hasPrice = true;
                    break;
                case "THUMB_IMAGE":
                    hasThumb_image = true;
                    break;
                case "CATEGORY_PATHS":
                    hasCategory_paths = true;
                    break;
                case "BRAND":
                    hasBrand = true;
                    break;
                case "VARIANT":
                    containsVariants = true;
                    break;
                default:
            }
        }

        if (!hasPid) {
            LOG.error("validateFieldMapConfig: no field defined for pid, exiting.");
            return false;
        }
        if (!hasTitle) {
            LOG.error("validateFieldMapConfig: no field defined for title, exiting.");
            return false;
        }
        if (!hasDescription) {
            LOG.error("validateFieldMapConfig: no field defined for description, exiting.");
            return false;
        }
        if (!hasUrl) {
            LOG.warn("validateFieldMapConfig: no field defined for url, just a warning.");
        }
        if (!hasAvailability) {
            LOG.warn("validateFieldMapConfig: no field defined for availability, just a warning.");
        }
        if (!hasPrice) {
            LOG.warn("validateFieldMapConfig: no field defined for price, just a warning.");
        }
        if (!hasThumb_image) {
            LOG.warn("validateFieldMapConfig: no field defined for thumb_image, just a warning.");
        }
        if (!hasBrand) {
            LOG.warn("validateFieldMapConfig: no field defined for brand, just a warning.");
        }

        if ("connect".equals(outputFormat)) {
            if (!hasCrumbs) {
                LOG.warn("validateFieldMapConfig: no field defined for crumbs, just a warning.");
            }
            if (!hasCrumbs_id) {
                LOG.warn("validateFieldMapConfig: no field defined for crumbs_id, just a warning.");
            }
        }

        if ("dataConnect".equals(outputFormat)) {
            if (!hasCategory_paths) {
                LOG.error("validateFieldMapConfig: no field defined for category_paths, exiting.");
                return false;
            }
        }
        if (!hasVariants && containsVariants) {
            LOG.error("validateFieldMapConfig: configured as no variants, but fieldMap contains fields defined with targetEntity=variant, exiting.");
            return false;
        }

        return true;
    }

    /***********************************************/

    boolean validateLabel(String label) {

        // check reserved
        ArrayList<String> reserved = new ArrayList<String>();
        reserved.add("op");
        reserved.add("path");
        reserved.add("views");
        reserved.add("variants");
        reserved.add("attributes");

        boolean isReserved = reserved.contains(label);
        if (isReserved) {
            LOG.error("validateLabel: invalid - reserved word: " + label);
            return false;
        }

        // check alphanumeric
        boolean isAlpha = label.matches("^[a-zA-Z0-9_]*$");
        if (!isAlpha) {
            LOG.error("validateLabel: invalid - remove special characters, spaces, etc: " + label);
            return false;
        }

        // check starts with a letter
        if (!Character.isLetter(label.charAt(0))) {
            LOG.error("validateLabel: invalid - must start with a letter: " + label);
            return false;
        }

        return true;
    }

    /***********************************************/

    public void processJson(
        String cCustomerName,
        String cInputFile,
        String cOutputProductFile,
        String cOutputVariantFile,
        boolean hasVariants) {

        /* Show what we're doing */
        LOG.info("*************************************************");
        LOG.info("Customer Name: " + cCustomerName);
        LOG.info("File: " + cInputFile);
        LOG.info("Output product File: " + cOutputProductFile);
        LOG.info("Output variant file: " + cOutputVariantFile);
        LOG.info("Has variants: " + hasVariants);
        System.out.println();

        try (JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                new FileInputStream(cInputFile)))) {

            jsonReader.setLenient(true);
            jsonReader.beginArray();
            int numberOfRecords = 0;
            LOG.info("Starting transform...");

            /* output file(s) setup */
            Writer productWriter = new FileWriter(cOutputProductFile);
            Writer variantWriter = new FileWriter(cOutputVariantFile);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            // skip the rest during testing
            while (jsonReader.hasNext()) {

                JsonObject rawInput = gson.fromJson(jsonReader, JsonObject.class);

                /* process row rules */
                JsonObject postRowRules = processRowRules(cCustomerName, rawInput);

                if (postRowRules != null) {
                    /* process field rules */
                    LOG.debug("processJson: postRowRules: " + postRowRules);
                    JsonObject[] postFieldRules = processFieldRules(cCustomerName, postRowRules);

                    /* Check to see if product already exists - don't add if it does */
                    if (postFieldRules.length > 0 && !objectExistsInList("product", postFieldRules[0])) {
                        productList.add(postFieldRules[0]);
                        JsonElement jePid = postFieldRules[0].get("pid");
                        String pid = jePid.getAsString();
                        pidList.add(pid);
                        LOG.debug("Added pid: " + pid);
                    }
                    if (postFieldRules.length > 0 && hasVariants && !objectExistsInList("variant", postFieldRules[1])) {
                        variantList.add(postFieldRules[1]);
                        JsonElement jeSkuId = postFieldRules[1].get("sku_id");
                        String skuId = jeSkuId.getAsString();
                        skuIdList.add(skuId);
                        LOG.debug("Added skuId: " + skuId);
                    }
                }

                // status
                numberOfRecords++;
                if (numberOfRecords % 1000 == 0) {
                    LOG.info("records: " + numberOfRecords);
                } else {
                    LOG.debug("records: " + numberOfRecords);
                }
            }

            /* write transformed json to product and variant files */
            gson.toJson(productList, productWriter);

            if (hasVariants) {
                gson.toJson(variantList, variantWriter);
            }

            LOG.info("Transform complete.  Total Records Found: " + numberOfRecords);
            jsonReader.endArray();

            /* close output file */
            variantWriter.close();
            productWriter.close();
        } catch (IOException e) {
            LOG.error("processJson: " + e);
        }

        /* if not using variants, delete the empty file we just created */
        if (!hasVariants) {
            File f = new File(cOutputVariantFile);
            f.delete();
        }
    }

    /***********************************************/

    boolean objectExistsInList(String entity, JsonObject dataObj) {
        if ("product".equals(entity)) {
            JsonElement jePid = dataObj.get("pid");
            String pid = jePid.getAsString();

            for (String itemInList : pidList) {
                if (itemInList.equals(pid)) {
                    LOG.debug("objectExistsInList: found existing pid: " + pid);
                    return true; // already in list
                }
            }
        } else if ("variant".equals(entity)) {
            JsonElement jeSkuId = dataObj.get("sku_id");
            if (jeSkuId == null) {
                LOG.error("objectExistsInList: looking for sku_id in variant object, but it's not found.  Verify sku_id is mapped to variant (not product).");
                return true;
            }
            String skuId = jeSkuId.getAsString();

            for (String itemInList : skuIdList) {
                if (itemInList.equals(skuId)) {
                    LOG.debug("objectExistsInList: found existing sku_id: " + skuId);
                    return true; // already in list
                }
            }
        } else {
            LOG.warn("objectExistsInList: Invalid entity provided: " + entity);
        }

        return false;
    }

    /***********************************************/

    public JsonObject processRowRules(String cCustomerName, JsonObject dataObj) {

        JsonObject transformedJson = dataObj;
        Processors procs = new Processors();
        procs.setCustomer(cCustomerName);

        /* check for filter rules */
        Iterator<RowFilterConfig> filterIter = getRowMap().getFilterProcessors().iterator();
        while (filterIter.hasNext()) {
            RowFilterConfig filter = filterIter.next();
            transformedJson = procs.filterRow(filter, dataObj);
            if (transformedJson == null) {
                // we should return here for nulls because we don't want this at all
                LOG.debug("Row filter applied, returning null");
                return null;
            }
        }

        /* check for split rules */
        Iterator<RowSplitConfig> splitIter = getRowMap().getSplitProcessors().iterator();
        while (splitIter.hasNext()) {
            RowSplitConfig split = splitIter.next();
            transformedJson = procs.splitRow(split, dataObj);
        }

        /* check for merge rules */
        Iterator<RowMergeConfig> mergeIter = getRowMap().getMergeProcessors().iterator();
        while (mergeIter.hasNext()) {
            RowMergeConfig merge = mergeIter.next();
            transformedJson = procs.mergeRow(merge, dataObj);
        }

        return transformedJson;
    }

    /***********************************************/

    public JsonObject[] processFieldRules(String cCustomerName, JsonObject dataObj) {

        Processors procs = new Processors();
        procs.setCustomer(cCustomerName);

        JsonObject product = new JsonObject();
        JsonObject variant = new JsonObject();

        /* loop through fieldMap */
        Iterator<FieldMap> fieldMapIter = getFieldMap().iterator();
        while (fieldMapIter.hasNext()) {
            FieldMap fm = fieldMapIter.next();
            JsonElement fieldData = dataObj.get(fm.getSourceLabel());

            String targetLabel = fm.getTargetLabel();
            String targetEntity = fm.getTargetEntity();
            LOG.debug("Processing field: " + targetLabel + ", for entity: " + targetEntity);


            if (!"product".equals(targetEntity) && !"variant".equals(targetEntity)) {
                LOG.warn("processFieldRules: Unsupported targetEntity: " + targetEntity + ", " + targetLabel);
                LOG.warn("processFieldRules: Defaulting to product");
                targetEntity = "product";
            }

            /* check if this field should be ignored */
            if (fm.getIgnoreThisField()) {
                LOG.debug("Ignoring field: " + targetLabel);
                continue;
            }

            /* check if this has a ValueGenerator defined - expected that fieldData is null here, else normal */
            if (!fm.getValueGenerator().getName().isEmpty()) {
                fieldRulesValueGenerator(fm, procs, product, variant);
            } else if (fm.getIsAttribute()) {
                /* this is an attribute */
                fieldRulesAttribute(fm, fieldData, procs, product, variant);
            } else {
                fieldRulesNormalField(fm, fieldData, procs, product, variant);
            }
        }

        JsonObject[] arr = new JsonObject[2];
        arr[0] = product;
        arr[1] = variant;

        return arr;
    }

    /***********************************************/

    public void fieldRulesValueGenerator(FieldMap fm, Processors procs, JsonObject product, JsonObject variant) {
        ValueGeneratorConfig vgc = fm.getValueGenerator();
        String targetLabel = fm.getTargetLabel();
        String targetEntity = fm.getTargetEntity();

        if (vgc.getName().equals("PriceGenerator")) {
            LOG.debug("Calling PriceGenerator: " + targetLabel);
            Map<String, String> prices = procs.priceGenerator(vgc.getValues());
            String priceName = "";
            String salesPriceName = "";

            for (ValueGeneratorConfigItem item : vgc.getValues()) {
                if (item.getName().equals("priceName")) {
                    priceName = item.getValue();
                }
                if (item.getName().equals("salePriceName")) {
                    salesPriceName = item.getValue();
                }
            }

            /* add field to json */
            if ("product".equals(targetEntity)) {
                product.addProperty(priceName, Double.parseDouble(prices.get(priceName)));
                product.addProperty(salesPriceName, Double.parseDouble(prices.get(salesPriceName)));
            } else if ("variant".equals(targetEntity)) {
                variant.addProperty(priceName, Double.parseDouble(prices.get(priceName)));
                variant.addProperty(salesPriceName, Double.parseDouble(prices.get(salesPriceName)));
            }
        } else if (vgc.getName().equals("ViewGenerator")) {
            LOG.debug("Calling ViewGenerator: " + targetLabel);
            String viewIds = procs.viewGenerator(vgc.getValues());

            /* add field to json */
            if ("product".equals(targetEntity)) {
                product.addProperty(targetLabel, viewIds);
            } else if ("variant".equals(targetEntity)) {
                variant.addProperty(targetLabel, viewIds);
            }
        }
    }

    /***********************************************/

    public void fieldRulesAttribute(FieldMap fm, JsonElement fieldData, Processors procs, JsonObject product, JsonObject variant) {
        String targetLabel = fm.getTargetLabel();
        String targetEntity = fm.getTargetEntity();

        LOG.debug("Attribute field processing: " + targetLabel);

        if ("product".equals(targetEntity)) {
            if (getConfiguredProductAttributes()) {
                /* run String rules */
                if (null == fieldData) {
                    LOG.debug("fieldRulesAttribute: nothing found for product attribute (just that data wasn't provided): " + targetLabel);
                    return;
                }
                String fixed = processStringRules(fm, fieldData.getAsString(), procs, product);

                /* had to do this to allow for non-string attributes */
                /* if this is configured with multi (or CategoryPaths), the attributes have already been added */
                if (!hasMultiAttributeProcessor(fm) && !hasCategoryPathsProcessor(fm)) {
                    /* add as single attribute */
                    LOG.debug("fieldRulesAttribute: adding as attribute");
                    getAttributes().addAttributeToJson(product, targetLabel, fixed, targetEntity);
                }
            } else {
                LOG.error("Field marked as product attribute, but hasProductAttributes=false in properties file: " + targetLabel);
            }
        } else if ("variant".equals(targetEntity)) {
            if (getConfiguredVariantAttributes()) {
                /* run String rules */
                if (null == fieldData) {
                    LOG.debug("fieldRulesAttribute: nothing found for variant attribute (just that data wasn't provided): " + targetLabel);
                    return;
                }
                String fixed = processStringRules(fm, fieldData.getAsString(), procs, variant);

                /* had to do this to allow for non-string attributes */
                /* if this is configured with multi, the attributes have already been added */
                if (!hasMultiAttributeProcessor(fm)) {
                    /* add as single attribute */
                    getAttributes().addAttributeToJson(variant, targetLabel, fixed, targetEntity);
                }
            } else {
                LOG.error("Field marked as variant attribute, but hasVariantAttributes=false in properties file: " + targetLabel);
            }
        } else {
            LOG.error("Unsupported entity type: " + targetLabel);
        }
    }

    /***********************************************/

    public boolean hasCategoryPathsProcessor(FieldMap fm) {
        Iterator<ProcessorConfig> fieldProcessorIter = fm.getProcessors().iterator();
        while (fieldProcessorIter.hasNext()) {
            ProcessorConfig processorConfig = fieldProcessorIter.next();

            if ("CategoryPaths".equals(processorConfig.getName())) {
                return true;
            }
        }
        return false;
    }

    /***********************************************/

    public boolean hasMultiAttributeProcessor(FieldMap fm) {
        Iterator<ProcessorConfig> fieldProcessorIter = fm.getProcessors().iterator();
        while (fieldProcessorIter.hasNext()) {
            ProcessorConfig processorConfig = fieldProcessorIter.next();

            if ("MultiAttribute".equals(processorConfig.getName())) {
                return true;
            }
        }
        return false;
    }

    /***********************************************/

    public void fieldRulesNormalField(
        FieldMap fm,
        JsonElement fieldData,
        Processors procs,
        JsonObject product,
        JsonObject variant) {
        String targetLabel = fm.getTargetLabel();
        String targetEntity = fm.getTargetEntity();

        LOG.debug("No ValueGenerator configured, normal field processing: " + targetLabel);

        /* must have valid fieldData and targetDataType */
        if (fieldData != null && fm.getTargetDataType() != null) {
            try {
                String preRules = fieldData.getAsString();

                if ("String".equals(fm.getTargetDataType())) {

                    /* run String rules */
                    String fixed = "";
                    if ("product".equals(targetEntity)) {
                        fixed = processStringRules(fm, preRules, procs, product);
                    } else if ("variant".equals(targetEntity)) {
                        fixed = processStringRules(fm, preRules, procs, variant);
                    }

                    /* use default value? */
                    if (fixed == null || "".equals(fixed)) {
                        fixed = fm.getDefaultValue();
                    }

                    /* add field to json */
                    if ("product".equals(targetEntity)) {
                        product.addProperty(targetLabel, fixed);
                    } else if ("variant".equals(targetEntity)) {
                        variant.addProperty(targetLabel, fixed);
                    }
                } else if ("Double".equals(fm.getTargetDataType())) {
                    LOG.debug("Processing as double datatype: " + preRules);
                    double dblValue = Double.parseDouble(preRules);

                    /* use default value? */
                    if (dblValue == 0) {
                        dblValue = Double.parseDouble(fm.getDefaultValue());
                    }

                    /* add field to json */
                    if ("product".equals(targetEntity)) {
                        product.addProperty(targetLabel, dblValue);
                    } else if ("variant".equals(targetEntity)) {
                        variant.addProperty(targetLabel, dblValue);
                    }
                } else if ("Long".equals(fm.getTargetDataType())) {
                    LOG.debug("Processing as double datatype: " + preRules);
                    long longValue = Long.parseLong(preRules);

                    /* use default value? */
                    if (longValue == 0) {
                        longValue = Long.parseLong(fm.getDefaultValue());
                    }

                    /* add field to json */
                    if ("product".equals(targetEntity)) {
                        product.addProperty(targetLabel, longValue);
                    } else if ("variant".equals(targetEntity)) {
                        variant.addProperty(targetLabel, longValue);
                    }
                } else if ("Boolean".equals(fm.getTargetDataType())) {
                    LOG.debug("Processing as boolean datatype: " + preRules);
                    Boolean boolValue = Boolean.parseBoolean(preRules);

                    /* add field to json */
                    if ("product".equals(targetEntity)) {
                        product.addProperty(targetLabel, boolValue);
                    } else if ("variant".equals(targetEntity)) {
                        variant.addProperty(targetLabel, boolValue);
                    }
                }
            } catch (NumberFormatException nfe) {
                LOG.error("processFieldRules: " + nfe);
                LOG.error("SourceLabel: " + fm.getSourceLabel());
                LOG.error("Expecting: " + fm.getTargetDataType());
                LOG.error("Data(invalid): " + fieldData);
            }
        }
    }

    /***********************************************/

    public String processStringRules(FieldMap fm, String preRules, Processors procs, JsonObject dataObj) {
        /* loop for any extra processors (Strings only for now ???) */
        Iterator<ProcessorConfig> fieldProcessorIter = fm.getProcessors().iterator();
        String fixed = preRules;
        String targetEntity = fm.getTargetEntity();

        while (fieldProcessorIter.hasNext()) {
            ProcessorConfig processorConfig = fieldProcessorIter.next();
            String procName = processorConfig.getName();

            switch (procName) {
                case "CategoryPaths":
                    LOG.debug("CategoryPaths before: " + fixed);
                    fixed = procs.categoryPaths(processorConfig.getValues(), fixed, dataObj, getAttributes(), targetEntity);
                    LOG.debug("CategoryPaths after: " + fixed);
                    break;
                case "MultiAttribute":
                    LOG.debug("MultiAttribute before: " + fixed);
                    fixed = procs.multiAttribute(processorConfig.getValues(), fixed, dataObj, getAttributes(), targetEntity);
                    LOG.debug("MultiAttribute after: " + fixed);
                    break;
                case "SpecialCharacters":
                    LOG.debug("SpecialCharacters before: " + fixed);
                    fixed = procs.specialCharacters(processorConfig.getValues(), fixed);
                    LOG.debug("SpecialCharacters after: " + fixed);
                    break;
                case "StringReplace":
                    LOG.debug("StringReplace before: " + fixed);
                    fixed = procs.stringReplace(processorConfig.getValues(), fixed);
                    LOG.debug("StringReplace after: " + fixed);
                    break;
                case "PrependString":
                    LOG.debug("PrependString before: " + fixed);
                    fixed = procs.prependString(processorConfig.getValues(), fixed);
                    LOG.debug("PrependString after: " + fixed);
                    break;
                case "MakeAlphaNumeric":
                    LOG.debug("MakeAlphaNumeric before: " + fixed);
                    fixed = procs.makeAlphaNumeric(fixed);
                    LOG.debug("MakeAlphaNumeric after: " + fixed);
                    break;
                case "ToUpperCase":
                    LOG.debug("ToUpperCase before: " + fixed);
                    fixed = procs.toUpperCase(fixed);
                    LOG.debug("ToUpperCase after: " + fixed);
                    break;
                default:
            }
        }

        return fixed;
    }

    /***********************************************/

    public void performAnalysis(String cCustomerName, boolean hasVariants, String analysisDirectory) {
        LOG.info("Starting analysis...");

        /* setup results manager */
        TransformerResultsManager trm = new TransformerResultsManager();
        trm.setCustomerName(cCustomerName);
        trm.setFirestoreDB(getFirestoreDB());

        /* create results object */
        SuperResults sr = new SuperResults();
        sr.setCustomerName(cCustomerName);

        LOG.info("Total unique products: " + productList.size());
        sr.setTotalProducts(Long.valueOf(productList.size()));

        /* dump this out to tab delimited file so people can review in excel */
        LOG.info("Unique product attributes added: " + getAttributes().getProductAttributeList().size());
        sr.setTotalUniqueProductAttributes(Long.valueOf(getAttributes().getProductAttributeList().size()));

        if (getAttributes().getProductAttributeList().size() > 0) {

            /* sorting variant attributes by count */
            List<String> sortedProductList = new ArrayList<String>();
            getAttributes().getProductAttributeList().entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> sortedProductList.add(x.getKey() + " --> " + x.getValue()));
            sr.setTop50ProductAttributes(sortedProductList);

            try {
                FileWriter writer = new FileWriter(analysisDirectory + "uniqueProductAttributes.txt");
                for (Map.Entry<String, Integer> item: getAttributes().getProductAttributeList().entrySet()) {
                    writer.write(item.getKey() + ":" + item.getValue() + System.lineSeparator());
                }
                writer.close();
            } catch (IOException e) {
                LOG.error("performAnalysis: " + e);
            }
        }

        LOG.info("Unique product attribute values added: " + getAttributes().getProductAttributeValueList().size());

        if (getAttributes().getProductAttributeValueList().size() > 0) {
            try {
                FileWriter writer = new FileWriter(analysisDirectory + "uniqueProductAttributeValues.txt");
                for (Map.Entry<String, Integer> item: getAttributes().getProductAttributeValueList().entrySet()) {
                    writer.write(item.getKey() + ":" + item.getValue() + System.lineSeparator());
                }
                writer.close();
            } catch (IOException e) {
                LOG.error("performAnalysis: " + e);
            }
        }

        if (hasVariants) {
            LOG.info("Total unique variants: " + variantList.size());
            sr.setTotalVariants(Long.valueOf(variantList.size()));

            /* dump this out to tab delimited file so people can review in excel */
            LOG.info("Unique variant attributes added: " + getAttributes().getVariantAttributeList().size());
            sr.setTotalUniqueVariantAttributes(Long.valueOf(getAttributes().getVariantAttributeList().size()));

            if (getAttributes().getVariantAttributeList().size() > 0) {

                /* sorting variant attributes by count */
                List<String> sortedVariantList = new ArrayList<String>();
                getAttributes().getVariantAttributeList().entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .forEachOrdered(x -> sortedVariantList.add(x.getKey() + " --> " + x.getValue()));
                sr.setTop50VariantAttributes(sortedVariantList);

                try {
                    FileWriter writer = new FileWriter(analysisDirectory + "uniqueVariantAttributes.txt");
                    for (Map.Entry<String, Integer> item: getAttributes().getVariantAttributeList().entrySet()) {
                        writer.write(item.getKey() + ":" + item.getValue() + System.lineSeparator());
                    }
                    writer.close();
                } catch (IOException e) {
                    LOG.error("performAnalysis: " + e);
                }
            }

            LOG.info("Unique variant attribute values added: " + getAttributes().getVariantAttributeValueList().size());
            if (getAttributes().getVariantAttributeValueList().size() > 0) {
                try {
                    FileWriter writer = new FileWriter(analysisDirectory + "uniqueVariantAttributeValues.txt");
                    for (Map.Entry<String, Integer> item: getAttributes().getVariantAttributeValueList().entrySet()) {
                        writer.write(item.getKey() + ":" + item.getValue() + System.lineSeparator());
                    }
                    writer.close();
                } catch (IOException e) {
                    LOG.error("performAnalysis: " + e);
                }
            }
        }

        // update DB with results
        trm.writeResultsToDB(sr);

        LOG.info("Analysis complete...");
    }
}
