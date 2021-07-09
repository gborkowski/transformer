package com.br.transform;

import java.util.ArrayList;

public class SuperRowMap {

    private String customerName = "";

    private ArrayList<RowFilterConfig> filterProcessors = new ArrayList<RowFilterConfig>();

    private ArrayList<RowSplitConfig> splitProcessors = new ArrayList<RowSplitConfig>();

    private ArrayList<RowMergeConfig> mergeProcessors = new ArrayList<RowMergeConfig>();

    public String getCustomerName() {
        return this.customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public ArrayList<RowFilterConfig> getFilterProcessors() {
        return filterProcessors;
    }

    public void setFilterProcessors(ArrayList<RowFilterConfig> filterProcessors) {
        if (filterProcessors == null) {
            this.filterProcessors = new ArrayList<RowFilterConfig>();
        } else {
            this.filterProcessors = filterProcessors;
        }
    }

    public ArrayList<RowSplitConfig> getSplitProcessors() {
        return splitProcessors;
    }

    public void setSplitProcessors(ArrayList<RowSplitConfig> splitProcessors) {
        if (splitProcessors == null) {
            this.splitProcessors = new ArrayList<RowSplitConfig>();
        } else {
            this.splitProcessors = splitProcessors;
        }
    }

    public ArrayList<RowMergeConfig> getMergeProcessors() {
        return mergeProcessors;
    }

    public void setMergeProcessors(ArrayList<RowMergeConfig> mergeProcessors) {
        if (mergeProcessors == null) {
            this.mergeProcessors = new ArrayList<RowMergeConfig>();
        } else {
            this.mergeProcessors = mergeProcessors;
        }
    }
}
