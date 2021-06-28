package com.br.transform;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
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
        String delimiterEntity = "";

        /* getting config values */
        for (ProcessorConfigItem item : args) {
            if ("delimiterEntity".equals(item.getName())) {
                delimiterEntity = item.getValue();
            }
        }

        LOG.debug("MultiAttribute: in: " + in);
        String[] entities = in.split(delimiterEntity);
        int sizeOfEntityArr = entities.length;

        for (int x = 0; x < sizeOfEntityArr; x++) {
            LOG.debug("MultiAttribute: entities[" + x + "]" + entities[x]);

            String[] pairs = in.split(entities[x]);
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

    public String specialCharacters(ArrayList<ProcessorConfigItem> args, String in) {
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
            if (item.getName().equals("from")) {
                from = item.getValue();
            }
            if (item.getName().equals("to")) {
                to = item.getValue();
            }
        }

        return in.replaceAll(from, to);
    }
    /***********************************************/

    public String prependString(ArrayList<ProcessorConfigItem> args, String in) {
        String prependValue = "";

        for (ProcessorConfigItem item : args) {
            if (item.getName().equals("prependValue")) {
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
            if ("priceName".equals(item.getName())) {
                priceName = item.getValue();
            }
            if ("salePriceName".equals(item.getName())) {
                salesPriceName = item.getValue();
            }
            if ("min".equals(item.getName())) {
                min = Double.parseDouble(item.getValue());
            }
            if ("max".equals(item.getName())) {
                max = Double.parseDouble(item.getValue());
            }
            if ("percentOnSale".equals(item.getName())) {
                percentOnSale = Double.parseDouble(item.getValue());
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
