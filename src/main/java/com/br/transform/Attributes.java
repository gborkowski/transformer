package com.br.transform;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Attributes.
 * Manages attributes - keeping track of counts for analysis purposes
 *
 */

public class Attributes {
    private static final Logger LOG = LogManager.getLogger(Attributes.class);

    // unique product attributes names (with counts)
    private Map<String, Integer> productAttributes = new HashMap<String, Integer>();

    private Map<String, Integer> productAttributeValues = new HashMap<String, Integer>();

    private Map<String, Integer> variantAttributes = new HashMap<String, Integer>();

    private Map<String, Integer> variantAttributeValues = new HashMap<String, Integer>();

    public Map<String, Integer> getProductAttributeList() {
        return productAttributes;
    }

    public void addToProductAttributeList(String attrName) {
        if (!productAttributes.containsKey(attrName)) {
            productAttributes.put(attrName, new Integer(1));
        } else {
            Integer current = (Integer) productAttributes.get(attrName);
            productAttributes.replace(attrName, current, current + 1);
        }
    }

    // unique product attributes name:values (with counts)
    public Map<String, Integer> getProductAttributeValueList() {
        return productAttributeValues;
    }

    public void addToProductAttributeValueList(String attrNameValue) {
        if (!productAttributeValues.containsKey(attrNameValue)) {
            productAttributeValues.put(attrNameValue, new Integer(1));
        } else {
            Integer current = (Integer) productAttributeValues.get(attrNameValue);
            productAttributeValues.replace(attrNameValue, current, current + 1);
        }
    }

    // unique variant attributes names (with counts)
    public Map<String, Integer> getVariantAttributeList() {
        return variantAttributes;
    }

    public void addToVariantAttributeList(String attrName) {
        if (!variantAttributes.containsKey(attrName)) {
            variantAttributes.put(attrName, new Integer(1));
        } else {
            Integer current = (Integer) variantAttributes.get(attrName);
            variantAttributes.replace(attrName, current, current + 1);
        }
    }

    // unique variant attributes name:values (with counts)
    public Map<String, Integer> getVariantAttributeValueList() {
        return variantAttributeValues;
    }

    public void addToVariantAttributeValueList(String attrNameValue) {
        if (!variantAttributeValues.containsKey(attrNameValue)) {
            variantAttributeValues.put(attrNameValue, new Integer(1));
        } else {
            Integer current = (Integer) variantAttributeValues.get(attrNameValue);
            variantAttributeValues.replace(attrNameValue, current, current + 1);
        }
    }

    /***********************************************/

    public void addAttributeToJson(JsonObject dataObj, String attributeName, String attributeValue, String targetEntity) {

        /* values for new attribute */
        AttributeItem ai = new AttributeItem();
        ai.setName(attributeName);
        ai.setValue(attributeValue);

        ArrayList<AttributeItem> attrList = new ArrayList<AttributeItem>();
        attrList.add(ai);

        /* create as new JsonElement */
        Gson gson = new GsonBuilder().create();
        JsonElement newAttribute = gson.toJsonTree(ai, AttributeItem.class);
        JsonElement newAttributeList = gson.toJsonTree(attrList, ArrayList.class);

        // test if dataObj has property called 'attributes'
        if (dataObj.has("attributes")) {
            LOG.debug("Attributes exist, adding new attribute");
            JsonArray existing = dataObj.getAsJsonArray("attributes");
            existing.add(newAttribute);
        } else {
            LOG.debug("Creating attributes - did not exist before");
            dataObj.add("attributes", newAttributeList.getAsJsonArray());
        }

        // tracking stuff for analytics
        if ("product".equals(targetEntity)) {
            addToProductAttributeList(attributeName);
            addToProductAttributeValueList(attributeName + ":" + attributeValue);
        } else if ("variant".equals(targetEntity)) {
            addToVariantAttributeList(attributeName);
            addToVariantAttributeValueList(attributeName + ":" + attributeValue);
        }
    }

    /***********************************************/

    public void addCategoryPathsToJson(JsonObject dataObj, JsonArray categoryPaths, String targetEntity) {

        LOG.debug("addCategoryPathsToJson: " + categoryPaths);

        if (!"product".equals(targetEntity)) {
            LOG.error("addCategoryPathsToJson: Unsupported targetEntity: " + targetEntity);
        } else {
            // create category_paths as a JsonArray attribute (make sure it doesn't exist)
            // this method should only be called once / product

            /* create as new JsonElement */
            List<JsonElement> attrList = new ArrayList<JsonElement>();

            Gson gson = new GsonBuilder().create();
            JsonElement newAttribute = gson.toJsonTree(categoryPaths);
            JsonObject catPathsObj = new JsonObject();
            catPathsObj.add("category_paths", newAttribute);
            attrList.add(catPathsObj);
            JsonElement newAttributeList = gson.toJsonTree(attrList, ArrayList.class);

            // test if dataObj has property called 'attributes'
            if (dataObj.has("attributes")) {
                LOG.debug("addCategoryPathsToJson: Attributes exist, adding category_paths to it");
                JsonArray existing = dataObj.getAsJsonArray("attributes");
                existing.add(catPathsObj);
            } else {
                LOG.debug("addCategoryPathsToJson: Creating attributes - did not exist before");
                dataObj.add("attributes", newAttributeList.getAsJsonArray());
            }
        }

    }
}
