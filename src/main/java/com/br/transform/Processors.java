package com.br.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Processors.
 * Processors
 *
 */

public class Processors {
    private static final Logger LOG = LogManager.getLogger(Processors.class);

    private String customer = "";

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    /***********************************************/

    public JsonObject filterRow(RowFilterConfig config, JsonObject dataObj) {

        /* config properties */
        String sourceLabel = config.getSourceLabel();
        String sourceDatatype = config.getSourceDatatype();
        String operation = config.getOperation();
        String testValue = config.getTestValue();

        JsonElement fieldData = dataObj.get(sourceLabel);
        String preRules = "";
        if ("String".equals(sourceDatatype)) {
            preRules = fieldData.getAsString();
            if ("equals".equals(operation) && preRules.equals(testValue)) {
                // match - we should filter this out, so return null
                return null;
            } else if ("contains".equals(operation) && preRules.contains(testValue)) {
                // match - we should filter this out, so return null
                return null;
            } else if ("startsWith".equals(operation) && preRules.startsWith(testValue)) {
                // match - we should filter this out, so return null
                return null;
            } else if ("endsWith".equals(operation) && preRules.endsWith(testValue)) {
                // match - we should filter this out, so return null
                return null;
            } else {
                LOG.error("FilterRow, operation not supported: " + operation);
            }
        } else {
            LOG.error("FilterRow, sourceDatatype not supported: " + sourceDatatype);
        }

        return dataObj;
    }

    /***********************************************/

    public JsonObject splitRow(RowSplitConfig config, JsonObject dataObj) {

        /* config properties */
        String sourceLabel = config.getSourceLabel();
        String sourceDatatype = config.getSourceDatatype();
        String targetLabelA = config.getTargetLabelA();
        String targetLabelB = config.getTargetLabelB();
        String delimiter = config.getDelimiter();
        String positionA = config.getPositionA();
        String positionB = config.getPositionB();

        JsonElement fieldData = dataObj.get(sourceLabel);
        String toBeSplit = "";
        if ("String".equals(sourceDatatype)) {
            int posA = Integer.parseInt(positionA);
            int posB = Integer.parseInt(positionB);
            toBeSplit = fieldData.getAsString();

            /* split into array */
            String[] arr = toBeSplit.split(delimiter);
            int sizeOfArr = arr.length;

            if (posA + 1 > sizeOfArr - 1 || posB + 1 > sizeOfArr) {
                LOG.error("SplitRow, index of positions defined are bad, sizeOfArray after split: " + sizeOfArr);
                LOG.error("SplitRow, data: " + toBeSplit);
            } else {
                /* add two new properties */
                dataObj.addProperty(targetLabelA, arr[posA]);
                dataObj.addProperty(targetLabelB, arr[posB]);
            }
        } else {
            LOG.error("SplitRow, sourceDatatype not supported: " + sourceDatatype);
        }

        return dataObj;
    }

    /***********************************************/

    public JsonObject mergeRow(RowMergeConfig config, JsonObject dataObj) {

        /* config properties */
        String sourceLabelA = config.getSourceLabelA();
        String sourceDatatypeA = config.getSourceDatatypeA();
        String sourceLabelB = config.getSourceLabelB();
        String sourceDatatypeB = config.getSourceDatatypeB();
        String targetLabel = config.getTargetLabel();
        String delimiter = config.getDelimiter();

        JsonElement fieldDataA = dataObj.get(sourceLabelA);
        JsonElement fieldDataB = dataObj.get(sourceLabelB);

        if ("String".equals(sourceDatatypeA) && "String".equals(sourceDatatypeB)) {
            String stringA = fieldDataA.getAsString();
            String stringB = fieldDataB.getAsString();

            String merged = stringA + delimiter + stringB;

            /* add merged property to object */
            dataObj.addProperty(targetLabel, merged);
        } else {
            LOG.error("MergeRow, sourceDatatype not supported: " + sourceDatatypeA + ", " + sourceDatatypeB);
        }
        return dataObj;
    }

    /***********************************************/

    public String multiAttribute(
        ArrayList<ProcessorConfigItem> args,
        String in,
        JsonObject dataObj,
        Attributes attribute,
        String targetEntity) {
        String secondDelimiter = "";
        String delimiterNameValue = "";

        /* getting config values */
        for (ProcessorConfigItem item : args) {
            if ("secondDelimiter".equals(item.getName())) {
                secondDelimiter = item.getValue();
            }
            if ("delimiterNameValue".equals(item.getName())) {
                delimiterNameValue = item.getValue();
            }
        }

        LOG.debug("MultiAttribute: in: " + in);
        String[] entities = in.split(secondDelimiter);
        int sizeOfEntityArr = entities.length;

        for (int x = 0; x < sizeOfEntityArr; x++) {
            LOG.debug("MultiAttribute: entities[" + x + "]" + entities[x]);

            String[] pairs = entities[x].split(delimiterNameValue);
            if (pairs.length == 2) {
                String name = pairs[0];
                String value = pairs[1];

                if (name != null && value != null) {
                    LOG.debug("MultiAttribute: name: " + name + ", value: " + value);
                    attribute.addAttributeToJson(dataObj, name, value, targetEntity);
                } else {
                    LOG.debug("MultiAttribute: something is null, not adding.  name: " + name + ", value: " + value);
                }
            } else {
                LOG.debug("MultiAttribute: didn't parse into usable name / value pairs." + pairs);
            }
        }
        return in;
    }

    /***********************************************/

    public String categoryPaths(
        ArrayList<ProcessorConfigItem> args,
        String in,
        JsonObject dataObj,
        Attributes attribute,
        String targetEntity) {
        String firstDelimiter = "";
        String secondDelimiter = "";
        String thirdDelimiter = "";
        String pattern = "";

        /* getting config values */
        for (ProcessorConfigItem item : args) {
            String itemName = item.getName();
            switch (itemName) {
                case "firstDelimiter":
                    firstDelimiter = item.getValue();
                    break;
                case "secondDelimiter":
                    secondDelimiter = item.getValue();
                    break;
                case "thirdDelimiter":
                    thirdDelimiter = item.getValue();
                    break;
                case "pattern":
                    pattern = item.getValue();
                    break;
                default:
                    break;
            }
        }

        /* decide which pattern to use */
        List<JsonArray> fullCategoryList = new ArrayList<JsonArray>();
        if ("pattern1".equals(pattern)) {
            fullCategoryList = categoryPathsPattern1(args, in, dataObj, attribute, targetEntity, firstDelimiter, secondDelimiter, thirdDelimiter);
        } else if ("pattern2".equals(pattern)) {
            fullCategoryList = categoryPathsPattern2(args, in, dataObj, attribute, targetEntity, firstDelimiter, secondDelimiter, thirdDelimiter);
        } else {
            LOG.error("categoryPaths: Unsupported pattern: " + pattern);
        }

        // add this as an attribute
        JsonArray fullCategoryPathsJA = new Gson().toJsonTree(fullCategoryList).getAsJsonArray();
        attribute.addCategoryPathsToJson(dataObj, fullCategoryPathsJA, targetEntity);

        return in;
    }

    /***********************************************/

    public List<JsonArray> categoryPathsPattern1(
        ArrayList<ProcessorConfigItem> args,
        String in,
        JsonObject dataObj,
        Attributes attribute,
        String targetEntity,
        String firstDelimiter,
        String secondDelimiter,
        String thirdDelimiter) {

        /*
        * Assumes input that looks like: “Mens:101>Clothes:102>Shirts:201,Mens:101>Clothes:102>Summer:301”
        */

        // Loop for each category
        LOG.debug("categoryPathsPattern1: in: " + in);
        List<JsonArray> fullCategoryList = new ArrayList<JsonArray>();
        String[] categories = in.split(firstDelimiter);
        int sizeOfCategoryArr = categories.length;

        for (int catIndex = 0; catIndex < sizeOfCategoryArr; catIndex++) {
            LOG.debug("categoryPathsPattern1: categories[" + catIndex + "]" + categories[catIndex]);

            // Loop for each entity within each category
            List<CategoryItem> categoryList = new ArrayList<CategoryItem>();
            String[] entities = categories[catIndex].split(secondDelimiter);
            int sizeOfEntityArr = entities.length;

            // parse string to construct and array of CategoryItems
            for (int entityIndex = 0; entityIndex < sizeOfEntityArr; entityIndex++) {
                LOG.debug("categoryPathsPattern1: entities[" + entityIndex + "]" + entities[entityIndex]);

                String[] pairs = entities[entityIndex].split(thirdDelimiter);
                if (pairs.length == 2) {
                    String name = pairs[0];
                    String id = pairs[1];
                    CategoryItem ci = new CategoryItem();
                    ci.setName(name);
                    ci.setId(id);
                    LOG.debug("categoryPathsPattern1: name: " + name + ", value: " + id);

                    // add this ci to the list
                    categoryList.add(ci);

                } else {
                    LOG.debug("categoryPathsPattern1: didn't parse into usable name / value pairs." + pairs);
                }
            }

            // convert list to jsonarray
            JsonArray categoryPathsJA = new Gson().toJsonTree(categoryList).getAsJsonArray();
            fullCategoryList.add(categoryPathsJA);
        }

        return fullCategoryList;
    }

    /***********************************************/

    public List<JsonArray> categoryPathsPattern2(
        ArrayList<ProcessorConfigItem> args,
        String in,
        JsonObject dataObj,
        Attributes attribute,
        String targetEntity,
        String firstDelimiter,
        String secondDelimiter,
        String thirdDelimiter) {

        /*
        * Assumes input that looks like: “Mens>Clothes>Shirts,Mens>Clothes>Summer:101>102>201,101>102>301”
        * basically crumbs:crumbs_id (names <firstDelimiter> ID's)
        * firstDelimiter=:
        * secondDelimiter=,
        * thirdDelimiter=>
        */

        // Split with firstDelimiter to separate names and ID's
        List<JsonArray> fullCategoryList = new ArrayList<JsonArray>();
        LOG.debug("categoryPathsPattern2: in: " + in);
        String[] namesAndIds = in.split(firstDelimiter);
        String names = namesAndIds[0];
        String ids = namesAndIds[1];

        // split names and ids into arrays
        String[] nameArray = names.split(secondDelimiter);
        String[] idArray = ids.split(secondDelimiter);

        // test they're the same length
        int sizeOfNameArr = nameArray.length;
        int sizeOfIdArr = idArray.length;

        if (sizeOfNameArr != sizeOfIdArr) {
            LOG.error("categoryPathsPattern2: name and id array lengths do not match.  Must be the same for pattern2");
        } else {

            // 2 dimensional arraylist
            ArrayList<ArrayList<CategoryItem>> graph = new ArrayList<>(sizeOfNameArr);

            // part one of populating the graph (add lists of category items - names only)
            for (int nameIndex = 0; nameIndex < sizeOfNameArr; nameIndex++) {
                LOG.debug("categoryPathsPattern2: nameArray[" + nameIndex + "]" + nameArray[nameIndex]);
                String[] nameItemArray = nameArray[nameIndex].split(thirdDelimiter);
                int sizeOfNameItemArr = nameItemArray.length;

                ArrayList<CategoryItem> nameItems = new ArrayList<>(sizeOfNameItemArr);
                for (int nameItemIndex = 0; nameItemIndex < nameItemArray.length; nameItemIndex++) {
                    CategoryItem ci = new CategoryItem();
                    ci.setName(nameItemArray[nameItemIndex]);
                    nameItems.add(ci);
                }
                graph.add(nameItems);
            }

            // part two, set the id for appropriate category items
            for (int idIndex = 0; idIndex < sizeOfNameArr; idIndex++) {
                LOG.debug("categoryPathsPattern2: idArray[" + idIndex + "]" + idArray[idIndex]);
                String[] idItemArray = idArray[idIndex].split(thirdDelimiter);
                int sizeOfIdItemArr = idItemArray.length;

                // these exist from previous loop
                ArrayList<CategoryItem> nameItems = graph.get(idIndex);
                for (int idItemIndex = 0; idItemIndex < sizeOfIdItemArr; idItemIndex++) {
                    // updating id for every name
                    nameItems.get(idItemIndex).setId(idItemArray[idItemIndex]);
                }
            }

            // loop to build up json
            for (ArrayList<CategoryItem> item : graph) {
                // convert list to jsonarray
                JsonArray categoryPathsJA = new Gson().toJsonTree(item).getAsJsonArray();
                fullCategoryList.add(categoryPathsJA);
            }
        }

        return fullCategoryList;
    }

    /***********************************************/

    public String specialCharacters(ArrayList<ProcessorConfigItem> args, String in) {
        String customerName = "";

        for (ProcessorConfigItem item : args) {
            if ("customerName".equals(item.getName())) {
                customerName = item.getValue();
            }
        }

        LOG.info("specialCharacters: customerName: " + customerName);

        /*
            Things to consider:
                - quotes (single and double) - escape them
                - what else?
        */

        return in;
    }

    /***********************************************/

    public String stringReplace(ArrayList<ProcessorConfigItem> args, String in) {
        String from = "";
        String to = "";

        for (ProcessorConfigItem item : args) {
            if ("from".equals(item.getName())) {
                from = item.getValue();
            }
            if ("to".equals(item.getName())) {
                to = item.getValue();
            }
        }

        return in.replaceAll(from, to);
    }
    /***********************************************/

    public String prependString(ArrayList<ProcessorConfigItem> args, String in) {
        String prependValue = "";

        for (ProcessorConfigItem item : args) {
            if ("prependValue".equals(item.getName())) {
                prependValue = item.getValue();
            }
        }

        return prependValue + in;
    }

    /***********************************************/

    public String toUpperCase(String in) {
        return in.toUpperCase();
    }

    /***********************************************/

    public String makeAlphaNumeric(String in) {
        return in.replaceAll("[^a-zA-Z0-9]", "");
    }

    /***********************************************/

    public String viewGenerator(ArrayList<ValueGeneratorConfigItem> args) {
        String viewIds = "";

        for (ValueGeneratorConfigItem item : args) {
            Float f = Float.parseFloat(item.getValue()) / 100;
            if (f == 1) {
                // base
                if (show(f)) {
                    viewIds += item.getName();
                }
            } else {
                if (show(f)) {
                    viewIds += "," + item.getName();
                }
            }
        }

        return viewIds;
    }

    private boolean show(float percent) {
        Random r = new Random();
        float chance = r.nextFloat();
        return chance <= percent;
    }

    /***********************************************/

    public Map<String, String> priceGenerator(ArrayList<ValueGeneratorConfigItem> args) {
        Map<String, String> prices = new HashMap<String, String>();

        double min = 0;
        double max = 0;
        double percentOnSale = 0;
        String priceName = "";
        String salesPriceName = "";

        for (ValueGeneratorConfigItem item : args) {
            String itemName = item.getName();
            switch (itemName) {
                case "priceName":
                    priceName = item.getValue();
                    break;
                case "salePriceName":
                    salesPriceName = item.getValue();
                    break;
                case "min":
                    min = Double.parseDouble(item.getValue());
                    break;
                case "max":
                    max = Double.parseDouble(item.getValue());
                    break;
                case "percentOnSale":
                    percentOnSale = Double.parseDouble(item.getValue());
                    break;
                default:
                    LOG.error("priceGenerator: invalid item name");
            }
        }

        double price = getFakePrice(min, max);
        double salePrice = getFakeSalePrice(price, percentOnSale);

        DecimalFormat df = new DecimalFormat("#.00");
        prices.put(priceName, df.format(price));
        prices.put(salesPriceName, df.format(salePrice));

        return prices;
    }

    public Double getFakePrice(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max + 1); // random price within defined range
    }

    public Double getFakeSalePrice(double price, double percentOnSale) {

        // this function takes a price and makes it 20% off, 20% of the time this is called
        Double salePrice = price;
        Random r = new Random();
        float chance = r.nextFloat();

        // it's randomly on sale (20% of the time)
        if (chance <= (percentOnSale / 100)) {
            salePrice = salePrice * .8; // 20 % off
        }

        return salePrice;
    }
}
