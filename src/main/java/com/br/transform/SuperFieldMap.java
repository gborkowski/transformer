package com.br.transform;

import java.util.ArrayList;
import java.util.List;

public class SuperFieldMap {

    private String customerName = "";

    private List<FieldMap> fieldMaps = new ArrayList<FieldMap>();

    public String getCustomerName() {
        return this.customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public List<FieldMap> getFieldMaps() {
        return fieldMaps;
    }

    public void setFieldMaps(List<FieldMap> fieldMaps) {
        this.fieldMaps = fieldMaps;
    }

}
