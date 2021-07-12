package com.br.transform;

import java.util.ArrayList;
import java.util.List;

public class SuperResults {

    private String customerName = "";

    private Long totalProducts = 0L;

    private Long totalVariants = 0L;

    private Long totalUniqueProductAttributes = 0L;

    private Long totalUniqueVariantAttributes = 0L;

    private List<String> top50ProductAttributes = new ArrayList<String>();

    private List<String> top50VariantAttributes = new ArrayList<String>();

    public List<String> getTop50ProductAttributes() {
        int max = this.top50ProductAttributes.size();
        if (max > 50) {
            max = 50;
        }

        return this.top50ProductAttributes.subList(0, max);
    }

    public void setTop50ProductAttributes(List<String> top50ProductAttributes) {
        this.top50ProductAttributes = top50ProductAttributes;
    }

    public List<String> getTop50VariantAttributes() {
        int max = this.top50VariantAttributes.size();
        if (max > 50) {
            max = 50;
        }

        return this.top50VariantAttributes.subList(0, max);
    }

    public void setTop50VariantAttributes(List<String> top50VariantAttributes) {
        this.top50VariantAttributes = top50VariantAttributes;
    }
    public Long getTotalProducts() {
        return this.totalProducts;
    }

    public void setTotalProducts(Long totalProducts) {
        this.totalProducts = totalProducts;
    }

    public Long getTotalVariants() {
        return this.totalVariants;
    }

    public void setTotalVariants(Long totalVariants) {
        this.totalVariants = totalVariants;
    }

    public Long getTotalUniqueProductAttributes() {
        return this.totalUniqueProductAttributes;
    }

    public void setTotalUniqueProductAttributes(Long totalUniqueProductAttributes) {
        this.totalUniqueProductAttributes = totalUniqueProductAttributes;
    }

    public Long getTotalUniqueVariantAttributes() {
        return this.totalUniqueVariantAttributes;
    }

    public void setTotalUniqueVariantAttributes(Long totalUniqueVariantAttributes) {
        this.totalUniqueVariantAttributes = totalUniqueVariantAttributes;
    }
    public String getCustomerName() {
        return this.customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }
}
