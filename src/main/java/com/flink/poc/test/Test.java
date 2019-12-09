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
import org.apache.flink.types.Row;

import java.util.Properties;


public class Test {

    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
            // System.out.println("### list obj " + listing);
            return listing;
        }).keyBy("listingId").flatMap(new UniqueListing());
//        System.out.println("### printing stream ");
//        stream.print();
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
        }).keyBy("agentId").flatMap(new UniqueAgent());
        return agentStream;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = setProps();
        DataStream<Listing> listingStream = processListings(properties);
        tEnv.registerDataStream("Listings", listingStream, "listingId, " +
                "earnestPayableTo, " +
                "statusChangeDate, " +
                "inclusions, " +
                "county, " +
                "agentId, " +
                "termsOffered, " +
                "nbrOfAcres, " +
                "coListingMemberUrl, " +
                "coListAgentId, " +
                "listOfficeBoardCode, " +
                "list207");
//        Table result2 = tEnv.sqlQuery(
//                "SELECT * FROM Listings");
        //   tEnv.toAppendStream(result2, Listing.class).print();
//        Table result3 = tEnv.sqlQuery("SELECT COUNT(*) FROM Listings");
//        tEnv.toRetractStream(result3, Long.class).print();

//        Table lresult2 = tEnv.sqlQuery(
//                "SELECT * FROM Listings WHERE listingId='L1002'");
//        tEnv.toAppendStream(lresult2, Listing.class).print();


        DataStream<Agent> agentStream = processAgents(properties);
        tEnv.registerDataStream("Agents", agentStream, "agentId, " +
                "city, " +
                "officeId, " +
                "email, " +
                "renegotiationExp, " +
                "nrdsid, " +
                "mlsStatus, " +
                "agentTimestamp");

        Table uAgents = tEnv.sqlQuery("SELECT * FROM Agents WHERE (agentId, agentTimestamp) " +
                "IN (SELECT agentId, MAX(agentTimestamp) FROM Agents GROUP BY agentId)");
        tEnv.registerTable("uAgentsTbl", uAgents);
                tEnv.toRetractStream(uAgents, Agent.class).print();

        Table result2 = tEnv.sqlQuery(
                "SELECT * FROM Agents");
           tEnv.toAppendStream(result2, Agent.class).print();

        Table result3 = tEnv.sqlQuery("SELECT COUNT(*) FROM Agents");
        tEnv.toRetractStream(result3, Long.class).print();

//        Table aresult2 = tEnv.sqlQuery(
//                "SELECT * FROM Agents WHERE agentId='A222'");
//        tEnv.registerTable("aresult2_tbl", aresult2);
//        tEnv.toAppendStream(aresult2, Agent.class).print();

        Table jinOut = tEnv.sqlQuery("SELECT * FROM Listings l " +
                "LEFT JOIN uAgentsTbl aa ON l.agentId = aa.agentId " +
                "LEFT JOIN uAgentsTbl ab ON l.coListAgentId = ab.agentId");
        tEnv.toRetractStream(jinOut, Row.class).print();
        env.execute("test-job");

    }

}
