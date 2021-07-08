package com.br.transform;

import com.google.gson.stream.JsonWriter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ImportParseDelimited.
 * Handles parsing delimited files
 *
 */

public class ImportParseDelimited {
    private static final Logger LOG = LogManager.getLogger(ImportParseDelimited.class);

    public void processDelimited(String cFileType, String cInputFile, String cFieldSeparator, boolean headerRowExists,
        String cOutputFile, String cCharacterSet, ImportHeaderInfo ihi) {

        /* Show what we're doing */
        LOG.info("*************************************************");
        LOG.info("File: " + cInputFile);
        LOG.info("File Type: " + cFileType);
        LOG.info("Output File: " + cOutputFile);
        LOG.info("Separator: " + cFieldSeparator);
        LOG.info("Header row exists: " + headerRowExists);
        LOG.info("Charset: " + cCharacterSet);
        System.out.println();

        Path myPath = Paths.get(cInputFile);
        try (BufferedReader br = Files.newBufferedReader(myPath, Charset.forName(cCharacterSet))) {
            LOG.info("Setting up...");

            char separator = ' ';
            if ("\t".equals(cFieldSeparator) || "TAB".equals(cFieldSeparator)) {
                separator = '\t';
            } else {
                separator = cFieldSeparator.charAt(0);
            }

            CSVParser parser = new CSVParserBuilder()
                .withSeparator(separator)
                .withQuoteChar('\"')
                .withIgnoreLeadingWhiteSpace(true)
                .build();

            CSVReader csvReader = new CSVReaderBuilder(br)
                .withCSVParser(parser)
                .withSkipLines(0)
                .build();

            // call method to get / create header - return arraylist
            String[] first = csvReader.peek();
            List<String> headerList = getHeaderKeys(first, headerRowExists, ihi);

            /* output file setup */
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream(cOutputFile)));
            writer.setIndent("    ");

            int numberOfRecords = 0;

            LOG.info("Starting parse...");

            // write opening square bracket
            writer.beginArray();

            Iterator<String[]> rows = csvReader.iterator();

            while (rows.hasNext()) {
                String[] line = rows.next();

                if (numberOfRecords == 0 && headerRowExists) {
                    LOG.info("Skipping header row");
                    numberOfRecords++;
                    continue;
                }

                // write opening curly bracket
                writer.beginObject();

                for (int x = 0; x < headerList.size(); x++) {
                    if (line[x] != null) {
                        writer.name(headerList.get(x)).value(line[x]);
                    }
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
            csvReader.close();

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

    /***********************************************/

    public List<String> getHeaderKeys(String[] first, boolean headerRowExists, ImportHeaderInfo ihi) {
        List<String> arr = new ArrayList<String>();

        if (headerRowExists) {
            // this means there is a header row so use that
            for (int x = 0; x < first.length; x++) {
                FieldMap fm = new FieldMap();
                String tmp = makeAlphaNumeric(first[x]);
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
                    arr.add(tmp);
                    ihi.addToHeaders(fm);
                }
            }
        } else {
            // generate header names
            for (int x = 0; x < first.length; x++) {
                FieldMap fm = new FieldMap();
                String tmp = "column" + x;
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
                    arr.add(tmp);
                    ihi.addToHeaders(fm);
                }
            }
        }
        return arr;
    }
}
