package com.br.transform;

/**
 * RowFilterConfig.
 * Holds row filter config
 *
 */

public class RowFilterConfig {

    private String sourceLabel = "";

    private String sourceDatatype = "";

    private String operation = "";

    private String testValue = "";

    public String getSourceLabel() {
        return sourceLabel;
    }

    public void setSourceLabel(String sourceLabel) {
        this.sourceLabel = sourceLabel;
    }

    public String getSourceDatatype() {
        return sourceDatatype;
    }

    public void setSourceDatatype(String sourceDatatype) {
        this.sourceDatatype = sourceDatatype;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getTestValue() {
        return testValue;
    }

    public void setTestValue(String testValue) {
        this.testValue = testValue;
    }
}
