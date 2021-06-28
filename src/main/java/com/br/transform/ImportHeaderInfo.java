package com.br.transform;

import java.util.ArrayList;
import java.util.List;

/**
 * ImportHeaderInfo.
 * Holds header info
 *
 */

public class ImportHeaderInfo {

    /* stores headers for import file */
    private List<FieldMap> headers = new ArrayList<FieldMap>();

    public List<FieldMap> getHeaders() {
        return headers;
    }

    public void setHeaders(List<FieldMap> headers) {
        this.headers = headers;
    }

    public void addToHeaders(FieldMap additional) {
        String inKey = additional.getSourceLabel();

        boolean exists = false;
        for (int x = 0; x < this.headers.size(); x++) {
            String current = this.headers.get(x).getSourceLabel();
            if (current.equals(inKey)) {
                exists = true;
            }
        }

        if (!exists) {
            this.headers.add(additional);
        }
    }
}
