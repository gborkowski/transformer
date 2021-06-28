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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OutputDataConnect {
    private static final Logger LOG = LogManager.getLogger(OutputDataConnect.class);

    /* variant list - loaded into memory */
    private List<JsonObject> variantList = new ArrayList<JsonObject>();

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
                if (numberOfRecords % 1000 == 0) {
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
                variantList.add(jsonObj);

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

            // write opening square bracket
            writer.beginArray();

            while (jsonReader.hasNext()) {
                JsonObject jsonObj = gson.fromJson(jsonReader, JsonObject.class);

                // get keys for json object
                Set<String> keys = jsonObj.keySet();
                Iterator<String> it = keys.iterator();

                // write opening curly bracket
                writer.beginObject();

                while (it.hasNext()) {
                    String key = it.next();
                    switchOnElementType(writer, jsonObj.get(key), key);
                }

                // handle adding variants
                if (hasVariants) {
                    handleVariants(writer, jsonObj);
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

    public void handleVariants(JsonWriter writer, JsonObject jsonObj) {
        // see if there are any matching by pid
        JsonElement pid = jsonObj.get("pid");
        LOG.debug("Getting variants for pid: " + pid);

        // temp holder for matches
        List<JsonObject> matching = new ArrayList<JsonObject>();

        for (JsonObject variant: variantList) {
            JsonElement variantPid = variant.get("pid");
            if (pid.equals(variantPid)) {
                matching.add(variant);
            }
        }

        try {
            // if yes, create <variants>
            if (matching.size() > 0) {
                LOG.debug("Starting to add variants");
                writer.name("variants");

                /* begin variants array */
                writer.beginArray();

                for (JsonObject var: matching) {
                    Set<String> keys = var.keySet();
                    Iterator<String> it = keys.iterator();

                    /* start this variant */
                    writer.beginObject();

                    while (it.hasNext()) {
                        String key = it.next();
                        switchOnElementType(writer, var.get(key), key);
                    }

                    /* end of this variant */
                    writer.endObject();
                }

                /* end variants */
                writer.endArray();
            }
        } catch (IOException ioe) {
            LOG.error("handleVariants: " + ioe);
        }
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
        LOG.debug("handleObject: Not implemented: " + key);
    }

    /***********************************************/

    public void handlePrimitive(JsonWriter writer, JsonPrimitive jp, String key) {
        try {
            LOG.debug("handlePrimitive: " + key + ", " + jp.getAsString());
            writer.name(key).value(jp.getAsString());
        } catch (IOException e) {
            LOG.error("handlePrimitive: " + e);
        }
    }
}
