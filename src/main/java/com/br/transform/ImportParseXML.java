package com.br.transform;

import com.google.gson.stream.JsonWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * ImportParseXML.
 * Handles parsing XML files
 *
 */

public class ImportParseXML {
    private static final Logger LOG = LogManager.getLogger(ImportParseXML.class);

    /* ImportHeaderInfo - global object */
    private ImportHeaderInfo globalHeaderInfo;

    /* preserving these - list */
    private List<String> preserveList = new ArrayList<String>();

    /* temp hashmap to store current variant info */
    private Map<String, String> variantInfo = new HashMap<String, String>();

    /* list of variants for current loop */
    private List<Map<String, String>> variantList = new ArrayList<Map<String, String>>();

    /* stores items for outputting to json when loop is complete */
    private Map<String, String> outputObject = new HashMap<String, String>();

    public Map<String, String> getOutputObject() {
        return outputObject;
    }

    public void setOutputObject(Map<String, String> outputObject) {
        this.outputObject = outputObject;
    }

    public void clearOutputObject() {
        this.outputObject.clear();
    }

    public void addToOutputObject(String key, String value, int counter) {
        FieldMap fm = new FieldMap();
        fm.setTargetEntity("product");
        fm.setTargetDataType("String");

        if (outputObject.containsKey(key) && preserveList.contains(key)) {

            // duplicate, so create new key so we don't overwrite (if key is in preserveList)
            if (outputObject.containsKey(key + String.valueOf(counter))) {
                this.addToOutputObject(key, value, counter + 1);
            } else {
                this.outputObject.put(key + String.valueOf(counter), value);
                fm.setSourceLabel(key + String.valueOf(counter));
                fm.setTargetLabel(key + String.valueOf(counter));
                globalHeaderInfo.addToHeaders(fm);
            }
        } else {
            this.outputObject.put(key, value);
            fm.setSourceLabel(key);
            fm.setTargetLabel(key);
            globalHeaderInfo.addToHeaders(fm);
        }
    }

    public List<Map<String, String>> getVariantList() {
        return variantList;
    }

    public void processXML(
        String cFileType,
        String cInputFile,
        String cOutputFile,
        String cCharacterSet,
        String cMainXMLEntity,
        String cProductXMLEntity,
        String cVariantXMLEntity,
        boolean hasVariants,
        String cPreserveValuesForSourceEntities,
        ImportHeaderInfo ihi) {

        /* Show what we're doing */
        LOG.info("*************************************************");
        LOG.info("File: " + cInputFile);
        LOG.info("File Type: " + cFileType);
        LOG.info("Output File: " + cOutputFile);
        LOG.info("Charset: " + cCharacterSet);
        LOG.info("Main XML Entity: " + cMainXMLEntity);
        LOG.info("Product XML Entity: " + cProductXMLEntity);
        LOG.info("Variant XML Entity: " + cVariantXMLEntity);
        LOG.info("Has Variants: " + hasVariants);
        LOG.info("Preserving: " + cPreserveValuesForSourceEntities);
        System.out.println();

        /* setting IHI */
        globalHeaderInfo = ihi;

        // copy list of preserve fields into array
        this.preserveList = Arrays.asList(cPreserveValuesForSourceEntities.split("\\s*,\\s*"));

        // Instantiate the Factory
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        try (InputStream is = new FileInputStream(cInputFile)) {
            LOG.info("Starting parse...");

            // parse XML file
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(is);

            /* output file setup */
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream(cOutputFile)));
            writer.setIndent("    ");

            // write opening square bracket
            writer.beginArray();

            if (doc.hasChildNodes()) {
                handleNodeList(
                    writer,
                    doc.getChildNodes(),
                    cMainXMLEntity,
                    cProductXMLEntity,
                    cVariantXMLEntity,
                    hasVariants);
            }

            // write closing square bracket
            writer.endArray();

            /* close output file */
            writer.close();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            LOG.error("ImportParseXML: processXML: " + e);
        }
    }

    /***********************************************/

    private  void handleNodeList(
        JsonWriter writer,
        NodeList nodeList,
        String cMainXMLEntity,
        String cProductXMLEntity,
        String cVariantXMLEntity,
        boolean hasVariants) {
        for (int count = 0; count < nodeList.getLength(); count++) {
            Node tempNode = nodeList.item(count);

            // make sure it's element node.
            if (tempNode.getNodeType() == Node.ELEMENT_NODE) {
                if (tempNode.hasChildNodes() || tempNode.getNodeType() != 3) {

                    // this indicates that this is a real value we want to get
                    if (tempNode.getChildNodes().getLength() == 1) {
                        String name = tempNode.getNodeName();
                        String value = tempNode.getTextContent();
                        if (tempNode.getParentNode().getNodeName().equals(cVariantXMLEntity)) {
                            /* adding to variantInfo object */
                            variantInfo.put(name, value);
                            FieldMap fm = new FieldMap();
                            fm.setTargetEntity("variant");
                            fm.setTargetDataType("String");
                            fm.setSourceLabel(name);
                            fm.setTargetLabel(name);
                            globalHeaderInfo.addToHeaders(fm);
                            LOG.debug("Added variant info name / value: " + name + " = " + value);
                        } else {
                            addToOutputObject(name, value, 1);
                            LOG.debug("Added product info name / value: " + name + " = " + value);
                        }
                    }

                    // loop again if has child nodes
                    handleNodeList(
                        writer,
                        tempNode.getChildNodes(),
                        cMainXMLEntity,
                        cProductXMLEntity,
                        cVariantXMLEntity,
                        hasVariants);
                }

                // this means we are at the end of variant
                if (tempNode.getNodeName().equals(cVariantXMLEntity)) {

                    /* add this variant to variantList - DOES this need to be cloned? */
                    getVariantList().add((HashMap<String, String>) variantInfo);
                    variantInfo.clear();
                }

                if (tempNode.getNodeName().equals(cProductXMLEntity)) {
                    // write the object that just closed
                    try {
                        /* loop for each item in variantList */
                        for (Map<String, String> variant : variantList) {

                            writer.beginObject();

                            /* write product level fields */
                            for (Map.Entry<String, String> entry : getOutputObject().entrySet()) {
                                writer.name(entry.getKey()).value(entry.getValue());
                            }

                            /* write variant level fields */
                            for (Map.Entry<String, String> variantEntry : variant.entrySet()) {
                                writer.name(variantEntry.getKey()).value(variantEntry.getValue());
                            }

                            writer.endObject();
                        }

                        // clear outputObject
                        variantList.clear();
                        clearOutputObject();
                    } catch (IOException ioe) {
                        LOG.error("handleNodeList(endObject): " + ioe);
                    }
                }
            }
        }
    }
    /*
        // ignoring attributes - here's an example if I need to grab them someday

        if (tempNode.hasAttributes()) {
            // get attributes names and values
            NamedNodeMap nodeMap = tempNode.getAttributes();
            for (int i = 0; i < nodeMap.getLength(); i++) {
                Node node = nodeMap.item(i);
                LOG.info("attr name : " + node.getNodeName());
                LOG.info("attr value : " + node.getNodeValue());
            }
        }
    */
}
