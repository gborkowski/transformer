package com.br.transform;

/**
 * RowMergeConfig.
 * Holds configuration for row merge processor
 *
 */

public class RowMergeConfig {

    private String sourceLabelA = "";

    private String sourceDatatypeA = "";

    private String sourceLabelB = "";

    private String sourceDatatypeB = "";

    private String targetLabel = "";

    private String delimiter = "";

    public String getSourceLabelA() {
        return sourceLabelA;
    }

    public void setSourceLabelA(String sourceLabelA) {
        this.sourceLabelA = sourceLabelA;
    }

    public String getSourceDatatypeA() {
        return sourceDatatypeA;
    }

    public void setSourceDatatypeA(String sourceDatatypeA) {
        this.sourceDatatypeA = sourceDatatypeA;
    }

    public String getSourceLabelB() {
        return sourceLabelB;
    }

    public void setSourceLabelB(String sourceLabelB) {
        this.sourceLabelB = sourceLabelB;
    }

    public String getSourceDatatypeB() {
        return sourceDatatypeB;
    }

    public void setSourceDatatypeB(String sourceDatatypeB) {
        this.sourceDatatypeB = sourceDatatypeB;
    }

    public String getTargetLabel() {
        return targetLabel;
    }

    public void setTargetLabel(String targetLabel) {
        this.targetLabel = targetLabel;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

}
