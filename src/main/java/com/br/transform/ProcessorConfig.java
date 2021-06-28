package com.br.transform;

import java.util.ArrayList;

/**
 * ProcessorConfig.
 * Holds configuration for processors
 *
 */

public class ProcessorConfig {

    private String name = "";

    private ArrayList<ProcessorConfigItem> values = new ArrayList<ProcessorConfigItem>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<ProcessorConfigItem> getValues() {
        return values;
    }

    public void setValues(ArrayList<ProcessorConfigItem> values) {
        this.values = values;
    }

}
