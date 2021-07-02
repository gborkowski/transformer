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
import org.apache.commons.lang3.StringUtils;
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
                LOG.warn("SplitRow, index of positions defined are bad, sizeOfArray after split: " + sizeOfArr);
                LOG.warn("SplitRow, data: " + toBeSplit);
                LOG.warn("SplitRow, Using single value for both targetA and targetB");
                dataObj.addProperty(targetLabelA, toBeSplit);
                dataObj.addProperty(targetLabelB, toBeSplit);
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
        String firstDelimiter = "";
        String secondDelimiter = "";
        String pattern = "";

        /* getting config values */
        for (ProcessorConfigItem item : args) {
            if ("firstDelimiter".equals(item.getName())) {
                firstDelimiter = item.getValue();
            }
            if ("secondDelimiter".equals(item.getName())) {
                secondDelimiter = item.getValue();
            }
            if ("pattern".equals(item.getName())) {
                pattern = item.getValue();
            }
        }

        LOG.debug("MultiAttribute: in: " + in);

        if ("pattern1".equals(pattern)) {
            multiAttributePattern1(args, in, dataObj, attribute, targetEntity, firstDelimiter, secondDelimiter);
        }
        if ("pattern2".equals(pattern)) {
            multiAttributePattern2(args, in, dataObj, attribute, targetEntity, firstDelimiter, secondDelimiter);
        }

        return in;
    }

    /***********************************************/

    public void multiAttributePattern1(
        ArrayList<ProcessorConfigItem> args,
        String in,
        JsonObject dataObj,
        Attributes attribute,
        String targetEntity,
        String firstDelimiter,
        String secondDelimiter) {

        /*
        * Assumes input that looks like: name:value>name:value>name:value”
        * firstDelimiter=>
        * secondDelimiter=:
        */
        LOG.debug("MultiAttribute: pattern 1");

        String[] entities = in.split(firstDelimiter);
        int sizeOfEntityArr = entities.length;

        for (int x = 0; x < sizeOfEntityArr; x++) {
            LOG.debug("MultiAttribute: entities[" + x + "]" + entities[x]);

            String[] pairs = entities[x].split(secondDelimiter);
            if (pairs.length == 2) {
                String name = pairs[0];

                /* have to clean up attribute names */
                name = name.replaceAll("-", "_");
                name = name.replaceAll("\\.", "");
                name = name.replaceAll(" ", "_");
                name = name.replaceAll("\\/", "_");
                String value = pairs[1];

                if (name != null && value != null) {
                    LOG.debug("MultiAttribute: name: " + name + ", value: " + value);
                    attribute.addAttributeToJson(dataObj, name, value, targetEntity);
                } else {
                    LOG.warn("MultiAttribute: something is null, not adding.  name: " + name + ", value: " + value);
                }
            } else {
                LOG.warn("MultiAttribute: didn't parse into usable name / value pairs." + pairs);
            }
        }
    }

    /***********************************************/

    public void multiAttributePattern2(
        ArrayList<ProcessorConfigItem> args,
        String in,
        JsonObject dataObj,
        Attributes attribute,
        String targetEntity,
        String firstDelimiter,
        String secondDelimiter) {

        /*
        ArrayList<String> topAttributes = new ArrayList<String>();
        topAttributes.add("Typ");
        topAttributes.add("Gewicht");
        topAttributes.add("Au");
        topAttributes.add("führung");
        topAttributes.add("Profil");
        topAttributes.add("Länge");
        topAttributes.add("Länge L");
        topAttributes.add("Zähnezahl");
        topAttributes.add("Teilung");
        topAttributes.add("Breite");
        topAttributes.add("Außen-Ø D");
        topAttributes.add("Breite B");
        topAttributes.add("Innen-Ø d");
        topAttributes.add("Paket");
        topAttributes.add("amtlänge");
        topAttributes.add("Gewinde");
        topAttributes.add("Innen-Ø");
        topAttributes.add("Betrieb");
        topAttributes.add("druck PN");
        topAttributes.add("chlu");
        topAttributes.add("Stärke");
        topAttributes.add("Farbe");
        topAttributes.add("Nennweite DN");
        topAttributes.add("Material");
        topAttributes.add("Anzahl Rippen");
        topAttributes.add("Spirallänge");
        topAttributes.add("Außen-Ø");
        topAttributes.add("tärke");
        topAttributes.add("Kettentyp");
        topAttributes.add("Antrieb");
        */

        /*
        * Specific to Haberkorn
        * Assumes input that looks like: A|B|C;R|D|G;X|U|K”
        * firstDelimiter=|
        * secondDelimiter=;
        * result: attribute A=BC, R=DG, X=UK
        */
        LOG.debug("MultiAttribute: pattern 2: " + in);

        String[] entities = in.split(firstDelimiter, 3);
        int sizeOfEntityArr = entities.length;

        if (sizeOfEntityArr == 3) {
            int count0 = StringUtils.countMatches(entities[0], secondDelimiter);
            int count1 = StringUtils.countMatches(entities[1], secondDelimiter);
            int count2 = StringUtils.countMatches(entities[2], secondDelimiter);

            LOG.debug("entities[0]: " + entities[0]);
            LOG.debug("entities[1]: " + entities[1]);
            LOG.debug("entities[2]: " + entities[2]);

            if (count0 == count1 && count1 == count2) {
                String[] sa_name_split = entities[0].split(secondDelimiter, -1);
                String[] sa_value_split = entities[1].split(secondDelimiter, -1); // -1 allows nulls - need this
                String[] sa_uom_split = entities[2].split(secondDelimiter, -1); // -1 allows nulls - need this

                for (int y = 0; y < sa_name_split.length; y++) {
                    //if (topAttributes.contains(sa_name_split[y])) {
                    if (sa_name_split.length > y) {
                        String name = StringUtils.stripAccents(sa_name_split[y]);

                        /* have to clean up attribute names */
                        name = name.replaceAll("-", "_");
                        name = name.replaceAll("\\.", "");
                        name = name.replaceAll(" ", "_");
                        name = name.replaceAll("\\/", "_");
                        LOG.debug("y: " + y);
                        LOG.debug("name: " + name);
                        LOG.debug("sa_name_split.length: " + sa_name_split.length);
                        LOG.debug("sa_value_split.length: " + sa_value_split.length);
                        LOG.debug("sa_uom_split.length: " + sa_uom_split.length);
                        String value = "";
                        if (sa_value_split.length > y) {
                            value = sa_value_split[y];
                            if (sa_uom_split.length > y) {
                                value += sa_uom_split[y];
                            }
                        }

                        if (name != null && value != null && !"".equals(name)) {
                            LOG.debug("MultiAttribute: name: " + name + ", value: " + value);
                            attribute.addAttributeToJson(dataObj, name, value, targetEntity);
                        }
                    } else {
                        LOG.warn("MultiAttribute: sa_name_split null (not enough array values): " + sa_name_split);
                    }
                    //}
                }
            } else {
                LOG.error("MultiAttribute: didn't parse into usable name / value pairs.  Counts didn't match");
            }
        } else {
            LOG.error("MultiAttribute: didn't parse into usable name / value pairs.  Split array size = " + sizeOfEntityArr + ": " + in);
        }
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
        String result = in;

        LOG.debug("specialCharacters: customerName: " + getCustomer());

        // fixes Haberkorn price field
        if ("Haberkorn".equals(getCustomer())) {
            String p = in.replaceAll(",", ".");
            String[] price = p.split("\\s");
            String goodPrice = price[0].replaceAll("€", "");
            int index = goodPrice.indexOf(".");
            result = goodPrice;

            if (goodPrice.indexOf(".") != goodPrice.lastIndexOf(".")) {
                result = goodPrice.substring(0, index) +  goodPrice.substring(index + 1);
            }
        }

        return result;
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
