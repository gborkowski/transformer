package com.br.transform;

import java.util.ArrayList;

/**
 * ValueGeneratorConfig.
 * Holds value generator config
 *
 */

public class ValueGeneratorConfig {

    private String name = "";

    private ArrayList<ValueGeneratorConfigItem> values = new ArrayList<ValueGeneratorConfigItem>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<ValueGeneratorConfigItem> getValues() {
        return values;
    }

    public void setValues(ArrayList<ValueGeneratorConfigItem> values) {
        this.values = values;
    }
}
