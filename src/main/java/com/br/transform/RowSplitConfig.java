package com.br.transform;

/**
 * RowSplitConfig.
 * Holds configuration for row splitting processor
 *
 */

public class RowSplitConfig {

    private String sourceLabel = "";

    private String sourceDatatype = "";

    private String targetLabelA = "";

    private String targetLabelB = "";

    private String delimiter = "";

    private String positionA = "";

    private String positionB = "";

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

    public String getTargetLabelA() {
        return targetLabelA;
    }

    public void setTargetLabelA(String targetLabelA) {
        this.targetLabelA = targetLabelA;
    }

    public String getTargetLabelB() {
        return targetLabelB;
    }

    public void setTargetLabelB(String targetLabelB) {
        this.targetLabelB = targetLabelB;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getPositionA() {
        return positionA;
    }

    public void setPositionA(String positionA) {
        this.positionA = positionA;
    }

    public String getPositionB() {
        return positionB;
    }

    public void setPositionB(String positionB) {
        this.positionB = positionB;
    }
}
