package com.flink.poc.test;

public class Agent {

    private String agentId;
    private String city;
    private String officeId;
    private String email;
    private String renegotiationExp;
    private String nrdsid;
    private String mlsStatus;
    private String agentTimestamp;

    public Agent() {

    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getOfficeId() {
        return officeId;
    }

    public void setOfficeId(String officeId) {
        this.officeId = officeId;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getRenegotiationExp() {
        return renegotiationExp;
    }

    public void setRenegotiationExp(String renegotiationExp) {
        this.renegotiationExp = renegotiationExp;
    }

    public String getNrdsid() {
        return nrdsid;
    }

    public void setNrdsid(String nrdsid) {
        this.nrdsid = nrdsid;
    }

    public String getMlsStatus() {
        return mlsStatus;
    }

    public void setMlsStatus(String mlsStatus) {
        this.mlsStatus = mlsStatus;
    }

    public String getAgentTimestamp() {
        return agentTimestamp;
    }

    public void setAgentTimestamp(String agentTimestamp) {
        this.agentTimestamp = agentTimestamp;
    }

    @Override
    public String toString() {
        return "Agent{" +
                "agentId='" + agentId + '\'' +
                ", city='" + city + '\'' +
                ", officeId='" + officeId + '\'' +
                ", email='" + email + '\'' +
                ", renegotiationExp='" + renegotiationExp + '\'' +
                ", nrdsid='" + nrdsid + '\'' +
                ", mlsStatus='" + mlsStatus + '\'' +
                ", timestamp='" + agentTimestamp + '\'' +
                '}';
    }

}
