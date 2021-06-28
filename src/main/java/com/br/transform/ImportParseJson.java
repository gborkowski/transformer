package com.br.transform;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ImportParseXML.
 * Handles parsing XML files
 *
 */

public class ImportParseJson {
    private static final Logger LOG = LogManager.getLogger(ImportParseJson.class);

    /***********************************************/

    public void processJson(String cFileType, String cInputFile, String cOutputFile, String cCharacterSet, ImportHeaderInfo ihi) {

        /* Show what we're doing */
        LOG.info("*************************************************");
        LOG.info("File: " + cInputFile);
        LOG.info("File Type: " + cFileType);
        LOG.info("Output File: " + cOutputFile);
        LOG.info("Charset: " + cCharacterSet);
        System.out.println();

        try (JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                new FileInputStream(cInputFile), Charset.forName(cCharacterSet)))) {

            Gson gson = new GsonBuilder().create();

            jsonReader.setLenient(true);
            jsonReader.beginArray();
            int numberOfRecords = 0;
            LOG.info("Starting parse...");

            /* output file setup */
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream(cOutputFile)));
            writer.setIndent("    ");

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
                    switchOnElementType(writer, jsonObj.get(key), key, ihi);
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

            LOG.info("Parse complete.  Total Records Found: " + numberOfRecords);
            jsonReader.endArray();

            /* close output file */
            writer.close();
        } catch (IOException e) {
            LOG.error("processJson: " + e);
        }
    }

    /***********************************************/

    public void switchOnElementType(JsonWriter writer, JsonElement je, String key, ImportHeaderInfo ihi) {

        if (je instanceof JsonArray) {
            // JSONARRAY
            handleArray(writer, je.getAsJsonArray(), key);
        } else if (je instanceof JsonObject) {
            // JSONOBJECT
            handleObject(writer, je.getAsJsonObject(), key, ihi);
        } else if (je instanceof JsonPrimitive) {
            // JSONPRIMITIVE
            handlePrimitive(writer, je.getAsJsonPrimitive(), key, ihi);
        } else {
            // UNKNOWN
            LOG.warn("Not sure what type of element this is: " + key);
        }
    }

    /***********************************************/

    public void handleArray(JsonWriter writer, JsonArray ja, String key) {

        try {
            LOG.debug("handleArray: " + key);

            // write name, then start array
            writer.name(key);

            FieldMap fm = new FieldMap();
            fm.setSourceLabel(key);
            fm.setTargetLabel(key);
            fm.setTargetDataType("String");
            fm.setTargetEntity("product");

            writer.beginArray();

            Iterator<JsonElement> it2 = ja.iterator();
            while (it2.hasNext()) {
                JsonElement elem = it2.next();
                // write each element of array
                writer.value(elem.getAsString());
            }
            writer.endArray();
        } catch (IOException e) {
            LOG.error("handleArray: " + e);
        }
    }

    /***********************************************/

    public void handleObject(JsonWriter writer, JsonObject jo, String key, ImportHeaderInfo ihi) {

        LOG.debug("handleObject: " + key);

        // handle objects (with children) by flattening them - only one level currently
        Set<Map.Entry<String, JsonElement>> js = jo.entrySet();
        Iterator<Map.Entry<String, JsonElement>> it3 = js.iterator();

        while (it3.hasNext()) {
            Map.Entry<String, JsonElement> child = it3.next();

            // some sick recursion
            switchOnElementType(writer, child.getValue(), key + "_" + child.getKey(), ihi);
        }
    }

    /***********************************************/

    public void handlePrimitive(JsonWriter writer, JsonPrimitive jp, String key, ImportHeaderInfo ihi) {
        FieldMap fm = new FieldMap();
        fm.setSourceLabel(key);
        fm.setTargetLabel(key);
        fm.setTargetEntity("product");

        try {
            LOG.debug("handlePrimitive: " + key);

            if (jp.isString()) {
                writer.name(key).value(jp.getAsString());
                fm.setTargetDataType("String");
            } else if (jp.isNumber()) {
                writer.name(key).value(jp.getAsNumber());
                fm.setTargetDataType("Number");
                fm.setDefaultValue("0");
            } else if (jp.isBoolean()) {
                writer.name(key).value(jp.getAsBoolean());
                fm.setTargetDataType("Boolean");
                fm.setDefaultValue("false");
            }
            ihi.addToHeaders(fm);
        } catch (IOException e) {
            LOG.error("handlePrimitive: " + e);
        }
    }
}
