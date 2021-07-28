package com.br.transform;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class OutputConnect {
    private static final Logger LOG = LogManager.getLogger(OutputConnect.class);

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
        LOG.info("Outputting to Connect (XML) format");
        System.out.println();

        OutputConnect oc = new OutputConnect();

        // if hasVariants, have to combine them
        if (hasVariants) {
            oc.loadVariants(cInputVariantFile);
        }

        // process products
        oc.processProducts(cInputProductFile, cOutputFile, hasVariants);
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
            LOG.error("loadVariants: " + e);
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
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.newDocument();
            Element rootElement = doc.createElementNS("https://www.bloomreach.com/product", "products");
            doc.appendChild(rootElement);


            while (jsonReader.hasNext()) {
                JsonObject jsonObj = gson.fromJson(jsonReader, JsonObject.class);

                // get keys for json object
                Set<String> keys = jsonObj.keySet();
                Iterator<String> it = keys.iterator();

                Element prod = doc.createElement("product");

                while (it.hasNext()) {
                    String key = it.next();
                    Element elem = doc.createElement(key);

                    switchOnElementType(doc, elem, jsonObj.get(key), key);

                    if (jsonObj.get(key) instanceof JsonPrimitive) {
                        String value = jsonObj.get(key).getAsJsonPrimitive().getAsString();

                        // if it's a primitive and it has a value, write it
                        if (value.length() > 0) {
                            // add this field (element) tp the product
                            prod.appendChild(elem);
                        }
                    } else {
                        // add this field (element) tp the product
                        prod.appendChild(elem);
                    }
                }

                // handle adding variants
                if (hasVariants) {
                    LOG.debug("Add variants here");
                    handleVariants(doc, prod, jsonObj);
                }

                // add product to root
                rootElement.appendChild(prod);

                // status
                numberOfRecords++;
                if (numberOfRecords % 500 == 0) {
                    LOG.info("records: " + numberOfRecords);
                } else {
                    LOG.debug("records: " + numberOfRecords);
                }
            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();

            // indents
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

            DOMSource source = new DOMSource(doc);

            //write to console or file
            StreamResult file = new StreamResult(new File(cOutputFile + ".xml"));

            //write data
            transformer.transform(source, file);

            LOG.info("Process complete.  Total Records Found: " + numberOfRecords);
            jsonReader.endArray();
        } catch (TransformerException | ParserConfigurationException | IOException e) {
            LOG.error("processJson: " + e);
        }
    }

    /***********************************************/

    public void handleVariants(Document doc, Element prod, JsonObject jsonObj) {
        // see if there are any matching by pid
        String pid = jsonObj.get("pid").getAsString();
        LOG.debug("Getting variants for pid: " + pid);

        // this is the MAX number of SKUS we allow per product (skip anything beyond this)
        int maxSkusPerProduct = 250;

        // temp holder for matches
        Collection<JsonObject> srcCollection = variantMap.get(pid);
        List<JsonObject> matching = new ArrayList<JsonObject>(srcCollection);

        if (matching.size() > maxSkusPerProduct) {
            LOG.warn("handleVariants: large number of variants for this pid: " + pid + ", count: " + matching.size());
        }

        // if yes, create <variants>
        if (matching.size() > 0) {
            Element variantsElement = doc.createElement("variants");

            int limitCounter = 0;
            for (JsonObject var: matching) {
                if (limitCounter <= maxSkusPerProduct) {
                    Set<String> keys = var.keySet();
                    Iterator<String> it = keys.iterator();

                    Element variantElement = doc.createElement("variant");

                    while (it.hasNext()) {
                        String key = it.next();
                        Element elem = doc.createElement(key);

                        switchOnElementType(doc, elem, var.get(key), key);

                        // add this field (element) tp the product
                        variantElement.appendChild(elem);
                    }

                    /* add variant to variants element */
                    variantsElement.appendChild(variantElement);
                }
                limitCounter++;
            }

            /* add variants to product */
            prod.appendChild(variantsElement);
        }
    }

    /***********************************************/

    public void switchOnElementType(Document doc, Element elem, JsonElement je, String key) {

        if (je instanceof JsonArray) {
            // JSONARRAY
            handleArray(doc, elem, je.getAsJsonArray(), key);
        } else if (je instanceof JsonObject) {
            // JSONOBJECT
            handleObject(doc, elem, je.getAsJsonObject(), key);
        } else if (je instanceof JsonPrimitive) {
            // JSONPRIMITIVE
            handlePrimitive(doc, elem, je.getAsJsonPrimitive(), key);
        } else {
            // UNKNOWN
            LOG.warn("Not sure what type of element this is: " + key);
        }
    }

    /***********************************************/

    public void handleArray(Document doc, Element elem, JsonArray ja, String key) {
        Gson gson = new Gson();

        LOG.debug("handleArray: " + key);

        Iterator<JsonElement> it2 = ja.iterator();
        while (it2.hasNext()) {
            JsonElement je = it2.next();
            AttributeItem ai = gson.fromJson(je, AttributeItem.class);
            LOG.debug("handleArray: AttributeItem: " + ai.getName() + ", " + ai.getValue());

            try {
                // write each element of array
                Element attr = doc.createElement(ai.getName());
                attr.appendChild(doc.createTextNode(ai.getValue()));

                // add to elem
                elem.appendChild(attr);
            } catch (DOMException de) {
                LOG.debug("Problem with adding XML attribute: " + de);
            }
        }
    }

    /***********************************************/

    public void handleObject(Document doc, Element elem, JsonObject jo, String key) {
        LOG.debug("handleObject: Not implemented: " + key);
    }

    /***********************************************/

    public void handlePrimitive(Document doc, Element elem, JsonPrimitive jp, String key) {

        LOG.debug("handlePrimitive: " + key + ", " + jp.getAsString());

        // no need to use escapeXml11 because this happens later
        // leaving this in there double escapes it and is not correct
        elem.appendChild(doc.createTextNode(jp.getAsString()));
    }
}
