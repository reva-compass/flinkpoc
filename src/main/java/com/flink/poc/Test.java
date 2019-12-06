package com.flink.poc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

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

    private static void processListings() {
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
            System.out.println("### list obj " + listing);
            return listing;
        });

        tEnv.registerDataStream("Orders", stream, "listingId, " +
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
        Table result2 = tEnv.sqlQuery(
                "SELECT * FROM Orders");
        tEnv.toAppendStream(result2, Listing.class).print();
        Table result3 = tEnv.sqlQuery("SELECT COUNT(*) FROM Orders");
        tEnv.toRetractStream(result3, Long.class).print();

//        Table listingsTable = tEnv.fromDataStream(stream, "listing_id, " +
//                "earnest_$_payable_to, " +
//                "status_change_date, " +
//                "inclusions, county, " +
//                "l_agent_id, terms_offered, " +
//                "nbr_of_acres, " +
//                "colisting_member_url, " +
//                "l_colist_agent_id, " +
//                "list_office_board_code, " +
//                "list_207");
//        Table result = tEnv.sqlQuery("SELECT COUNT(*) FROM listingsTable");
//        tEnv.registerTable("t_result", result);
//        tEnv.toAppendStream(result, Types.LONG).print();
    }

    public static void main(String[] args) throws Exception {
        processListings();
        env.execute("test-job");
    }


}
