package com.br.transform;

import com.google.gson.stream.JsonWriter;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ImportParseEndeca {
    private static final Logger LOG = LogManager.getLogger(ImportParseEndeca.class);

    public void processEndeca(String cFileType, String cInputFile, String cOutputFile, String cCharacterSet, ImportHeaderInfo ihi) {

        /* Show what we're doing */
        LOG.info("*************************************************");
        LOG.info("File: " + cInputFile);
        LOG.info("File Type: " + cFileType);
        LOG.info("Output File: " + cOutputFile);
        LOG.info("Charset: " + cCharacterSet);
        System.out.println();

        Path myPath = Paths.get(cInputFile);
        try (BufferedReader br = Files.newBufferedReader(myPath, Charset.forName(cCharacterSet))) {
            LOG.info("Setting up...");

            /* output file setup */
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream(cOutputFile)));
            writer.setIndent("    ");

            int numberOfRecords = 0;

            LOG.info("Starting parse...");

            // write opening square bracket
            writer.beginArray();

            // stores current records headers / values
            Map<String, String> headerNames = new HashMap<String, String>();

            String strLine = "";
            while ((strLine = br.readLine()) != null) {
                String[] line = strLine.split("\t");

                if (line[0] != null) {

                    if ("recordType".equals(line[0])) {
                        // this is the start of a new object
                        headerNames.clear();
                    } else if ("REC".equals(line[0])) {
                        // this is the end of the object, so write everything out

                        // write opening curly bracket
                        writer.beginObject();

                        // write out all name / values to json, and populate headers (for field maps)
                        for (Map.Entry<String, String> entry : headerNames.entrySet()) {
                            // write headers (for field maps)
                            FieldMap fm = new FieldMap();
                            String tmp = makeAlphaNumeric(entry.getKey());
                            fm.setSourceLabel(tmp);
                            fm.setTargetLabel(tmp);
                            fm.setTargetDataType("String");
                            fm.setTargetEntity("product");

                            // see if it already exists
                            boolean exists = false;
                            for (FieldMap existing : ihi.getHeaders()) {
                                String existingSourceLabel = existing.getSourceLabel();
                                if (existingSourceLabel.equals(tmp)) {
                                    exists = true;
                                }
                            }
                            if (!exists) {
                                ihi.addToHeaders(fm);
                                LOG.debug("added header: " + tmp);
                            }

                            // write json (with alphanumeric name)
                            writer.name(tmp).value(entry.getValue());
                        }

                        numberOfRecords++;

                        // write closing curly bracket
                        writer.endObject();
                    } else {
                        if (headerNames.containsKey(line[0])) {
                            // get current value
                            String current = headerNames.get(line[0]);

                            // append to existing value for this name
                            headerNames.put(line[0], current + "|" + line[1]);
                        } else {
                            headerNames.put(line[0], line[1]);
                        }
                    }
                }

                // status
                if (numberOfRecords % 1000 == 0) {
                    LOG.info("records: " + numberOfRecords);
                } else {
                    LOG.debug("records: " + numberOfRecords);
                }
            }

            // write closing square bracket
            writer.endArray();

            LOG.info("Parse complete.  Total Records Found: " + numberOfRecords);
            br.close();

            /* close output file */
            writer.close();

        } catch (IOException e) {
            LOG.error("processDelimited: " + e);
        }
    }

    /***********************************************/

    public String makeAlphaNumeric(String in) {
        return in.replaceAll("[^a-zA-Z0-9]", "");
    }

}
