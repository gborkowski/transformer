package com.br.transform;

import java.util.ArrayList;

/**
 * FieldMap.
 * Holds fieldmap info
 *
 */

public class FieldMap {

    private Boolean ignoreThisField = false;

    private String sourceLabel = "";

    private String targetLabel = "";

    private String targetDataType = "";

    private String targetEntity = "";

    private Boolean isAttribute = false;

    private String defaultValue = "";

    private String outputDelimiter = "";

    private ValueGeneratorConfig valueGenerator = new ValueGeneratorConfig();

    private ArrayList<ProcessorConfig> processors = new ArrayList<ProcessorConfig>();

    public Boolean getIgnoreThisField() {
        return ignoreThisField;
    }

    public void setIgnoreThisField(Boolean ignoreThisField) {
        this.ignoreThisField = ignoreThisField;
    }

    public String getSourceLabel() {
        return sourceLabel;
    }

    public void setSourceLabel(String sourceLabel) {
        this.sourceLabel = sourceLabel;
    }

    public String getTargetLabel() {
        return targetLabel;
    }

    public void setTargetLabel(String targetLabel) {
        this.targetLabel = targetLabel;
    }

    public String getTargetDataType() {
        return targetDataType;
    }

    public void setTargetDataType(String targetDataType) {
        this.targetDataType = targetDataType;
    }

    public String getTargetEntity() {
        return targetEntity;
    }

    public void setTargetEntity(String targetEntity) {
        this.targetEntity = targetEntity;
    }

    public Boolean getIsAttribute() {
        return isAttribute;
    }

    public void setIsAttribute(Boolean isAttribute) {
        this.isAttribute = isAttribute;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getOutputDelimiter() {
        return outputDelimiter;
    }

    public void setOutputDelimiter(String outputDelimiter) {
        this.outputDelimiter = outputDelimiter;
    }

    public ValueGeneratorConfig getValueGenerator() {
        return valueGenerator;
    }

    public void setValueGenerator(ValueGeneratorConfig valueGenerator) {
        this.valueGenerator = valueGenerator;
    }

    public ArrayList<ProcessorConfig> getProcessors() {
        return processors;
    }

    public void setProcessors(ArrayList<ProcessorConfig> processors) {
        this.processors = processors;
    }
}
