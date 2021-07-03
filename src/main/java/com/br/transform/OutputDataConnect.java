package com.br.transform;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OutputDataConnect {
    private static final Logger LOG = LogManager.getLogger(OutputDataConnect.class);

    /* variant map - loaded into memory */
    private MultiValuedMap<String, JsonObject> variantMap = new ArrayListValuedHashMap<>();

    /***********************************************/

    public void output(String cInputProductFile, String cInputVariantFile, boolean hasVariants, String cOutputFile) {

        /* Show what we're doing */
        LOG.info("*************************************************");
        LOG.info("Product File: " + cInputProductFile);
        LOG.info("Variant File: " + cInputVariantFile);
        LOG.info("Has Variants: " + hasVariants);
        LOG.info("Output File: " + cOutputFile);
        LOG.info("Outputting to DataConnect (JSONL) format");
        System.out.println();

        OutputDataConnect odc = new OutputDataConnect();

        // if hasVariants, have to combine them
        if (hasVariants) {
            odc.loadVariants(cInputVariantFile);
        }

        // process products
        odc.processProducts(cInputProductFile, cOutputFile, hasVariants);

        // covert json to jsonl
        odc.convertToJsonl(cOutputFile);
    }

    /***********************************************/

    public void convertToJsonl(String cJsonOutputFile) {
        try (JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                new FileInputStream(cJsonOutputFile + ".json"), Charset.forName("UTF8")))) {

            Gson gson = new GsonBuilder().create();
            jsonReader.setLenient(true);
            jsonReader.beginArray();
            int numberOfRecords = 0;
            LOG.info("Creating JSONL file...");

            /* output file setup */
            FileWriter writer = new FileWriter(new File(cJsonOutputFile + ".jsonl"));

            // write opening square bracket
            writer.write("[\n");

            while (jsonReader.hasNext()) {
                JsonObject jsonObj = gson.fromJson(jsonReader, JsonObject.class);
                String jsonStr = jsonObj.toString();
                LOG.debug(jsonStr);

                // write line
                writer.write(jsonStr);

                // handle comma or not
                if (jsonReader.hasNext()) {
                    writer.write(",\n");
                } else {
                    writer.write("\n");
                }

                // status
                numberOfRecords++;
                if (numberOfRecords % 500 == 0) {
                    LOG.info("records: " + numberOfRecords);
                } else {
                    LOG.debug("records: " + numberOfRecords);
                }
            }

            // write closing square bracket
            writer.write("]");

            LOG.info("Process complete.  Total Records Found: " + numberOfRecords);
            jsonReader.endArray();

            /* close output file */
            writer.close();
        } catch (IOException e) {
            LOG.error("processJson: " + e);
        }
    }

    /***********************************************/

    public void loadVariants(String cInputVariantFile) {
        try (JsonReader jsonReader = new JsonReader(
            new InputStreamReader(
            new FileInputStream(cInputVariantFile), Charset.forName("UTF8")))) {

            Gson gson = new GsonBuilder().create();
            jsonReader.setLenient(true);
            jsonReader.beginArray();
            int numberOfRecords = 0;
            LOG.info("Loading variants...");

            while (jsonReader.hasNext()) {
                JsonObject jsonObj = gson.fromJson(jsonReader, JsonObject.class);
                String variantPid = jsonObj.get("pid").getAsString();
                variantMap.put(variantPid, jsonObj);

                // status
                numberOfRecords++;
                if (numberOfRecords % 1000 == 0) {
                    LOG.info("records: " + numberOfRecords);
                } else {
                    LOG.debug("records: " + numberOfRecords);
                }
            }

            LOG.info("Loading complete.  Total Records Found: " + numberOfRecords);
            jsonReader.endArray();
        } catch (IOException e) {
            LOG.error("LoadVariants: " + e);
        }
    }

    /***********************************************/

    public void processProducts(String cInputProductFile, String cOutputFile, boolean hasVariants) {
        try (JsonReader jsonReader = new JsonReader(
            new InputStreamReader(
            new FileInputStream(cInputProductFile), Charset.forName("UTF8")))) {

            Gson gson = new GsonBuilder().create();
            jsonReader.setLenient(true);
            jsonReader.beginArray();
            int numberOfRecords = 0;
            LOG.info("Starting process...");

            /* output file setup */
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream(cOutputFile + ".json")));
            writer.setIndent("    ");

            // write opening square bracket
            writer.beginArray();

            while (jsonReader.hasNext()) {
                JsonObject jsonObj = gson.fromJson(jsonReader, JsonObject.class);

                // for dataConnect, have to format things in certain way
                JsonObject dcFormat = convertTransformedProductToDataConnectFormat(jsonObj, hasVariants);

                // write opening curly bracket
                writer.beginObject();

                // get keys for json object
                Set<String> keys = dcFormat.keySet();
                Iterator<String> it = keys.iterator();

                while (it.hasNext()) {
                    String key = it.next();
                    switchOnElementType(writer, dcFormat.get(key), key);
                }

                // write closing curly bracket
                writer.endObject();

                // status
                numberOfRecords++;
                if (numberOfRecords % 1000 == 0) {
                    LOG.info("records: " + numberOfRecords);
                } else {
                    LOG.debug("records: " + numberOfRecords);
                }
            }

            // write closing square bracket
            writer.endArray();

            LOG.info("Process complete.  Total Records Found: " + numberOfRecords);
            jsonReader.endArray();

            /* close output file */
            writer.close();
        } catch (IOException e) {
            LOG.error("processJson: " + e);
        }
    }

    /***********************************************/

    public JsonObject convertTransformedProductToDataConnectFormat(JsonObject transformed, boolean hasVariants) {

        // this is the main dc object we're formatting
        JsonObject dcFormat = new JsonObject();
        boolean hasViews = false;
        Gson gson = new Gson();

        // add op:add
        dcFormat.addProperty("op", "add");

        // add path:/products/$pid
        String pid = transformed.get("pid").getAsString();
        dcFormat.addProperty("path", "/products/" + pid);

        // create value and attributes
        JsonObject value = new JsonObject();
        JsonObject attributes = new JsonObject();

        // add product properties and attributes (if any) to attributes
        Set<String> keys = transformed.keySet();
        Iterator<String> it = keys.iterator();

        while (it.hasNext()) {
            String key = it.next();
            JsonElement je = transformed.get(key);

            // test if views is in the object - if so, do not treat like normal property
            if ("views".equals(key)) {
                hasViews = true;
                continue;
            }

            if (je instanceof JsonArray) {
                /* this must (or at least should) be the attributes array */
                JsonArray ja = je.getAsJsonArray();

                Iterator<JsonElement> it2 = ja.iterator();
                while (it2.hasNext()) {
                    JsonElement jeAttribute = it2.next();
                    AttributeItem ai = gson.fromJson(jeAttribute, AttributeItem.class);
                    LOG.debug("convertTransformedProductToDataConnectFormat: AttributeItem: " + ai.getName() + ", " + ai.getValue());

                    // write attribute property
                    attributes.addProperty(ai.getName(), ai.getValue());
                }
            } else if (je instanceof JsonObject) {
                LOG.error("convertTransformedProductToDataConnectFormat: json has a JsonObject - should not have this.");
            } else if (je instanceof JsonPrimitive) {
                // write normal property
                LOG.debug("convertTransformedProductToDataConnectFormat: Property: " + key + ", " + je.getAsString());
                attributes.addProperty(key, je.getAsString());
            } else {
                LOG.error("convertTransformedProductToDataConnectFormat: unsupported type");
            }
        }

        // add attributes to value
        value.add("attributes", attributes);

        // add variants to value
        if (hasVariants) {
            JsonObject variants = new JsonObject();
            variants = mapVariantsToDataConnectFormat(variants, transformed);
            if (variants != null) {
                value.add("variants", variants);
            }
        }

        // add views to value
        if (hasViews) {
            JsonObject views = new JsonObject();
            views = mapViews(views, transformed);
            if (views != null) {
                value.add("views", views);
            }
        }
        // add value as property to dcFormat
        dcFormat.add("value", value);

        return dcFormat;
    }

    /***********************************************/

    public JsonObject mapViews(JsonObject views, JsonObject transformed) {

        // for data connect, views must be named 'views'
        String viewStr = transformed.get("views").getAsString();

        // loop (comma delimited) viewStr to get view values
        String[] viewArr = viewStr.split(",");
        int viewArrLength = viewArr.length;
        if (viewArrLength == 0) {
            return null;
        }

        // create view object (empty) for each viewId
        for (int x = 0; x < viewArrLength; x++) {
            JsonObject v = new JsonObject();

            // add view objects to views
            views.add(viewArr[x], v);
        }

        return views;
    }

    /***********************************************/

    public JsonObject mapVariantsToDataConnectFormat(JsonObject variants, JsonObject transformed) {
        // copy variant properties and attributes into variants object in proper format
        Gson gson = new Gson();

        // see if there are any matching by pid
        String pid = transformed.get("pid").getAsString();
        LOG.debug("mapVariantsToDataConnectFormat: Getting variants for pid: " + pid);

        Collection<JsonObject> srcCollection = variantMap.get(pid);
        List<JsonObject> matching = new ArrayList<JsonObject>(srcCollection);

        if (matching.size() > 0) {
            LOG.debug("Starting to add variants");
            JsonObject variant = new JsonObject();
            JsonObject attributes = new JsonObject();

            for (JsonObject currentVariant: matching) {
                Set<String> keys = currentVariant.keySet();
                Iterator<String> it = keys.iterator();
                String skuId = "";

                // loop for each property and attributes within the current variant
                while (it.hasNext()) {
                    String key = it.next();
                    JsonElement je = currentVariant.get(key);

                    // get skuId
                    if ("sku_id".equals(key)) {
                        skuId = je.getAsString();
                    }

                    if (je instanceof JsonArray) {
                        /* this must (or at least should) be the attributes array */
                        JsonArray ja = je.getAsJsonArray();

                        Iterator<JsonElement> it2 = ja.iterator();
                        while (it2.hasNext()) {
                            JsonElement jeAttribute = it2.next();
                            AttributeItem ai = gson.fromJson(jeAttribute, AttributeItem.class);
                            LOG.debug("mapVariantsToDataConnectFormat: AttributeItem: " + ai.getName() + ", " + ai.getValue());

                            // write attribute property
                            attributes.addProperty(ai.getName(), ai.getValue());
                        }
                    } else if (je instanceof JsonObject) {
                        LOG.error("mapVariantsToDataConnectFormat: json has a JsonObject - should not have this.");
                    } else if (je instanceof JsonPrimitive) {
                        // write normal property
                        attributes.addProperty(key, je.getAsString());
                    } else {
                        LOG.error("mapVariantsToDataConnectFormat: unsupported type");
                    }
                }

                // add attributes to variant
                variant.add("attributes", attributes);

                // add variant to variants
                variants.add(skuId, variant);
            }
        } else {
            return null;
        }

        return variants;
    }

    /***********************************************/

    public void switchOnElementType(JsonWriter writer, JsonElement je, String key) {

        if (je instanceof JsonArray) {
            // JSONARRAY
            handleArray(writer, je.getAsJsonArray(), key);
        } else if (je instanceof JsonObject) {
            // JSONOBJECT
            handleObject(writer, je.getAsJsonObject(), key);
        } else if (je instanceof JsonPrimitive) {
            // JSONPRIMITIVE
            handlePrimitive(writer, je.getAsJsonPrimitive(), key);
        } else {
            // UNKNOWN
            LOG.warn("Not sure what type of element this is: " + key);
        }
    }

    /***********************************************/

    public void handleArray(JsonWriter writer, JsonArray ja, String key) {
        Gson gson = new Gson();

        try {
            LOG.debug("handleArray: " + key);

            // write name, then start array
            writer.name(key);

            writer.beginObject();

            Iterator<JsonElement> it2 = ja.iterator();
            while (it2.hasNext()) {
                JsonElement je = it2.next();
                AttributeItem ai = gson.fromJson(je, AttributeItem.class);
                LOG.debug("handleArray: AttributeItem: " + ai.getName() + ", " + ai.getValue());

                // write each element of array
                writer.name(ai.getName()).value(ai.getValue());
            }
            writer.endObject();
        } catch (IOException e) {
            LOG.error("handleArray: " + e);
        }
    }

    /***********************************************/

    public void handleObject(JsonWriter writer, JsonObject jo, String key) {
        LOG.debug("handleObject: " + key);

        try {
            writer.name(key);

            writer.beginObject();

            // handle objects (with children?)
            Set<Map.Entry<String, JsonElement>> js = jo.entrySet();
            Iterator<Map.Entry<String, JsonElement>> it = js.iterator();

            while (it.hasNext()) {
                Map.Entry<String, JsonElement> child = it.next();

                // some sick recursion
                switchOnElementType(writer, child.getValue(), child.getKey());
            }

            writer.endObject();

        } catch (IOException e) {
            LOG.error("handlePrimitive: " + e);
        }
    }

    /***********************************************/

    public void handlePrimitive(JsonWriter writer, JsonPrimitive jp, String key) {
        try {
            // don't write out pid or sku_id for dataConnect
            if (!"pid".equals(key) && !"sku_id".equals(key) && !"skuid".equals(key)) {
                LOG.debug("handlePrimitive: " + key + ", " + jp.getAsString());
                writer.name(key).value(jp.getAsString());
            }
        } catch (IOException e) {
            LOG.error("handlePrimitive: " + e);
        }
    }
}
