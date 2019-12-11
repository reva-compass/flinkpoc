package com.flink.poc.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Properties;


public class Test {

    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //    private static EnvironmentSettings settings = EnvironmentSettings.newInstance()
//            .useBlinkPlanner()
//            .inStreamingMode()
//            .build();
//    private static StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
//    private static EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    private static StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    private static Properties setProps() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092");
        return properties;
    }

    private static DataStream<Listing> processListings(Properties properties) {
        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>("poc_test_listing", new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<Listing> listingStream = env.addSource(kafkaConsumer).map((MapFunction<ObjectNode, Listing>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Listing listing = new Listing();
            listing.setListingId(jsonNode.get("Listing ID").textValue());
            listing.setEarnestPayableTo(jsonNode.get("Earnest $ Payable To").textValue());
            listing.setStatusChangeDate(jsonNode.get("Status Change Date").textValue());
            listing.setInclusions(jsonNode.get("Inclusions").textValue());
            listing.setCounty(jsonNode.get("County").textValue());
            listing.setAgentId(jsonNode.get("Agent ID").textValue());
            listing.setTermsOffered(jsonNode.get("Terms Offered").textValue());
            listing.setNbrOfAcres(jsonNode.get("Nbr of Acres").textValue());
            listing.setCoListingMemberUrl(jsonNode.get("CoListingMemberUrl").textValue());
            listing.setCoListAgentId(jsonNode.get("CoList Agent ID").textValue());
            listing.setListOfficeBoardCode(jsonNode.get("List Office Board Code").textValue());
            listing.setList207(jsonNode.get("LIST_207").textValue());
            return listing;
        }).keyBy("agentId");
        return listingStream;

    }

    private static DataStream<Agent> processAgents(Properties properties) {
        FlinkKafkaConsumer<ObjectNode> agentKafkaConsumer = new FlinkKafkaConsumer<>("poc_test_agent", new JSONKeyValueDeserializationSchema(true), properties);
        agentKafkaConsumer.setStartFromEarliest();
        DataStream<Agent> agentStream = env.addSource(agentKafkaConsumer).map((MapFunction<ObjectNode, Agent>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Agent agent = new Agent();
            agent.setAgentId(jsonNode.get("Agent ID").textValue());
            agent.setCity(jsonNode.get("City").textValue());
            agent.setOfficeId(jsonNode.get("Office ID").textValue());
            agent.setEmail(jsonNode.get("Email").textValue());
            agent.setRenegotiationExp(jsonNode.get("RENegotiation Exp").textValue());
            agent.setNrdsid(jsonNode.get("NRDSID").textValue());
            agent.setMlsStatus(jsonNode.get("MLS Status").textValue());
            agent.setAgentTimestamp(jsonNode.get("agent_timestamp").textValue());
            // System.out.println("### agent obj " + agent);
            return agent;
        }).keyBy("agentId");
        return agentStream;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = setProps();
        DataStream<Listing> listingStream = processListings(properties);
//        tEnv.registerDataStream("Listings", listingStream, "listingId, " +
//                "earnestPayableTo, " +
//                "statusChangeDate, " +
//                "inclusions, " +
//                "county, " +
//                "agentId, " +
//                "termsOffered, " +
//                "nbrOfAcres, " +
//                "coListingMemberUrl, " +
//                "coListAgentId, " +
//                "listOfficeBoardCode, " +
//                "list207");

        DataStream<Agent> agentStream = processAgents(properties);
//        tEnv.registerDataStream("Agents", agentStream, "agentId, " +
//                "city, " +
//                "officeId, " +
//                "email, " +
//                "renegotiationExp, " +
//                "nrdsid, " +
//                "mlsStatus, " +
//                "agentTimestamp");

        DataStream<List<JoinedListing>> enrichedListings = listingStream.connect(agentStream).flatMap(new EnrichmentFunction()).uid("enriched");
        enrichedListings.print();
        env.execute("test-job");

    }


}
