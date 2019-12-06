package com.flink.poc;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class Listing {

    private String listingId;
    private String earnestPayableTo;
    private String statusChangeDate;
    private String inclusions;
    private String county;
    private String agentId;
    private String termsOffered;
    private String nbrOfAcres;
    private String coListingMemberUrl;
    private String coListAgentId;
    private String listOfficeBoardCode;
    private String list207;

    public Listing() {

    }

//    public Listing(JsonNode jsonNode) {
//        System.out.println("## # listingid " + jsonNode.get("Listing ID").textValue());
//        listingId = jsonNode.get("Listing ID").textValue();
//        earnestPayableTo = jsonNode.get("Earnest $ Payable To").textValue();
//        statusChangeDate = jsonNode.get("Status Change Date").textValue();
//        inclusions = jsonNode.get("Inclusions").textValue();
//        county = jsonNode.get("County").textValue();
//        agentId = jsonNode.get("Agent ID").textValue();
//        termsOffered = jsonNode.get("Terms Offered").textValue();
//        nbrOfAcres = jsonNode.get("Nbr of Acres").textValue();
//        coListingMemberUrl = jsonNode.get("CoListingMemberUrl").textValue();
//        coListAgentId = jsonNode.get("CoList Agent ID").textValue();
//        listOfficeBoardCode = jsonNode.get("List Office Board Code").textValue();
//        list207 = jsonNode.get("LIST_207").textValue();
//    }


    public String getListingId() {
        return listingId;
    }

    public void setListingId(String listingId) {
        this.listingId = listingId;
    }

    public String getEarnestPayableTo() {
        return earnestPayableTo;
    }

    public void setEarnestPayableTo(String earnestPayableTo) {
        this.earnestPayableTo = earnestPayableTo;
    }

    public String getStatusChangeDate() {
        return statusChangeDate;
    }

    public void setStatusChangeDate(String statusChangeDate) {
        this.statusChangeDate = statusChangeDate;
    }

    public String getInclusions() {
        return inclusions;
    }

    public void setInclusions(String inclusions) {
        this.inclusions = inclusions;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String getTermsOffered() {
        return termsOffered;
    }

    public void setTermsOffered(String termsOffered) {
        this.termsOffered = termsOffered;
    }

    public String getNbrOfAcres() {
        return nbrOfAcres;
    }

    public void setNbrOfAcres(String nbrOfAcres) {
        this.nbrOfAcres = nbrOfAcres;
    }

    public String getCoListingMemberUrl() {
        return coListingMemberUrl;
    }

    public void setCoListingMemberUrl(String coListingMemberUrl) {
        this.coListingMemberUrl = coListingMemberUrl;
    }

    public String getCoListAgentId() {
        return coListAgentId;
    }

    public void setCoListAgentId(String coListAgentId) {
        this.coListAgentId = coListAgentId;
    }

    public String getListOfficeBoardCode() {
        return listOfficeBoardCode;
    }

    public void setListOfficeBoardCode(String listOfficeBoardCode) {
        this.listOfficeBoardCode = listOfficeBoardCode;
    }

    public String getList207() {
        return list207;
    }

    public void setList207(String list207) {
        this.list207 = list207;
    }

    @Override
    public String toString() {
        return "Listing{" +
                "listingId='" + listingId + '\'' +
                ", earnestPayableTo='" + earnestPayableTo + '\'' +
                ", statusChangeDate='" + statusChangeDate + '\'' +
                ", inclusions='" + inclusions + '\'' +
                ", county='" + county + '\'' +
                ", agentId='" + agentId + '\'' +
                ", termsOffered='" + termsOffered + '\'' +
                ", nbrOfAcres='" + nbrOfAcres + '\'' +
                ", coListingMemberUrl='" + coListingMemberUrl + '\'' +
                ", coListAgentId='" + coListAgentId + '\'' +
                ", listOfficeBoardCode='" + listOfficeBoardCode + '\'' +
                ", list207='" + list207 + '\'' +
                '}';
    }
}
