package com.flink.poc.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.Properties;

public class TemporalTest {


    private static Properties setProps() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092");
        return properties;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Properties properties = setProps();

        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>("poc_test_listing", new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<Listing> stream = env.addSource(kafkaConsumer).map((MapFunction<ObjectNode, Listing>) jsonNodes -> {
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
        });


        Table listingsHistory = tEnv.fromDataStream(stream, "listingId, " +
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
                "list207, " +
                "proctime.proctime");
        tEnv.registerTable("ListingsHistory", listingsHistory);

//        DataStream<String> listingIdsStream = env.addSource(kafkaConsumer).map((MapFunction<ObjectNode, String>) jsonNodes -> {
//            JsonNode jsonNode = jsonNodes.get("value");
//            return jsonNode.get("Listing ID").textValue();
//        });
//        Iterator<String> listingIdIter = DataStreamUtils.collect(listingIdsStream);
//        List<String> ids = new ArrayList<>();
//        while (listingIdIter.hasNext()) {
//            ids.clear();
//            ids.add(listingIdIter.next());
//        }
//        System.out.println("### ids " + ids);

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
            return agent;
        });
        Table agentsHistory = tEnv.fromDataStream(agentStream, "agentId, " +
                "city, " +
                "officeId, " +
                "email, " +
                "renegotiationExp, " +
                "nrdsid, " +
                "mlsStatus, " +
                "agentTimestamp, " +
                "proctime.proctime");
        tEnv.registerTable("AgentsHistory", agentsHistory);

        TemporalTableFunction listings = listingsHistory.createTemporalTableFunction("proctime", "listingId");
        tEnv.registerFunction("Listings", listings);

        TemporalTableFunction agents = agentsHistory.createTemporalTableFunction("proctime", "agentId");
        tEnv.registerFunction("Agents", agents);

        // Listings -> Rates

        //   String queryStr = "SELECT * FROM ListingsHistory as o, LATERAL TABLE (Agents(o.proctime)) as r WHERE r.agentId = o.agentId";
        String queryStr = "SELECT * FROM ListingsHistory as o " +
                "INNER JOIN LATERAL TABLE (Agents(o.proctime)) as r ON (r.agentId = o.agentId) " +
                "INNER JOIN LATERAL TABLE (Agents(o.proctime)) as ra ON (ra.agentId = o.coListAgentId)";
        Table result = tEnv.sqlQuery(queryStr);
        tEnv.registerTable("Result", result);
        tEnv.toAppendStream(result, Row.class).print();

        env.execute("test-job");

    }
}
