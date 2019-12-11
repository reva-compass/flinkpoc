package com.flink.poc.test;

public class JoinedListing {

    private Listing listing;
    private Agent listAgent;
    private Agent coListAgent;

    public JoinedListing() {
      //  System.out.println("### a new joined listing object.");
    }

    public Listing getListing() {
        return listing;
    }

    public void setListing(Listing listing) {
        this.listing = listing;
    }

    public Agent getListAgent() {
        return listAgent;
    }

    public void setListAgent(Agent listAgent) {
        this.listAgent = listAgent;
    }

    public Agent getCoListAgent() {
        return coListAgent;
    }

    public void setCoListAgent(Agent coListAgent) {
        this.coListAgent = coListAgent;
    }

    @Override
    public String toString() {
        return "JoinedListing{" +
                "listing=" + listing +
                ", listAgent=" + listAgent +
                ", coListAgent=" + coListAgent +
                '}';
    }
}
