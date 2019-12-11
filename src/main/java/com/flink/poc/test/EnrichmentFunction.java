package com.flink.poc.test;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

/*
NEXT TODO: its returning same value for a key for both agents and coagents map...
      see appending "a-" for agents key and "co-" for coagents key and see if the values are different.
 */

public class EnrichmentFunction extends RichCoFlatMapFunction<Listing, Agent, List<JoinedListing>> {

    ValueState<Listing> listing;
    ValueState<Agent> agent;
    MapState<String, Set<String>> agents;
    MapState<String, Set<String>> coAgents;
    MapState<String, JoinedListing> joinedListings;

    @Override
    public void open(Configuration config) throws IOException {
        TypeInformation<String> sTypeInfo = TypeInformation.of(String.class);
        TypeInformation<Set<String>> setTypeInfo = TypeInformation.of(new TypeHint<Set<String>>() {
        });
        TypeInformation<JoinedListing> jlTypeInfo = TypeInformation.of(JoinedListing.class);

        listing = getRuntimeContext().getState(new ValueStateDescriptor<>("listing-state", Listing.class));
        agent = getRuntimeContext().getState(new ValueStateDescriptor<>("agent-state", Agent.class));
        agents = getRuntimeContext().getMapState(new MapStateDescriptor<String, Set<String>>("agents-state", sTypeInfo, setTypeInfo));
        coAgents = getRuntimeContext().getMapState(new MapStateDescriptor<String, Set<String>>("co-agents-state", sTypeInfo, setTypeInfo));
        joinedListings = getRuntimeContext().getMapState(new MapStateDescriptor<String, JoinedListing>("joine-listings", sTypeInfo, jlTypeInfo));
//        if(listing != null && listing.value() != null) {
//            System.out.println("### Its LISTING");
//            System.out.println("### incoming listing id: " + listing.value().getListingId());
//            System.out.println("### incoming agent id: " + listing.value().getAgentId());
//        }
//        if(agent != null && agent.value() != null) {
//            System.out.println("### Its AGENT");
//            System.out.println("### incoming agent id: " + agent.value().getAgentId());
//        }

    }

    @Override
    public void flatMap1(Listing listing, Collector<List<JoinedListing>> out) throws Exception {

        String listingId = listing.getListingId();
        JoinedListing jl = joinedListings.get(listingId);
        String existingAgentId = null;
        String existingCoAgentId = null;
        if (jl == null) {
            jl = new JoinedListing();
            joinedListings.put(listingId, jl);
        } else {
            existingAgentId = jl.getListing().getAgentId();
            existingCoAgentId = jl.getListing().getCoListAgentId();
        }

        // When agent of a listing is updated, remove older agent -> listing mapping
        Set<String> listingsPerExistingAgent = agents.get(existingAgentId);
        if (listingsPerExistingAgent != null) {
            listingsPerExistingAgent.remove(listingId);
        }
        Set<String> listingsPerExistingCoListAgent = agents.get(existingCoAgentId);
        if (listingsPerExistingCoListAgent != null) {
            listingsPerExistingCoListAgent.remove(listingId);
        }

        // update agent -> listings mapping with new info
        List<JoinedListing> jls = new ArrayList<>();
        System.out.println("### listingId " + listingId);
        String agentId = listing.getAgentId();
        System.out.println("### agentId " + agentId);
        Set<String> listingsPerAgent = agents.get(agentId);
        if (listingsPerAgent == null) {
            listingsPerAgent = new HashSet<>();
            agents.put(agentId, listingsPerAgent);
        }
        listingsPerAgent.add(listingId);
        System.out.println("### listingsPerAgent " + listingsPerAgent);
        String coAgentId = listing.getCoListAgentId();
        System.out.println("### coAgentId " + coAgentId);
        if (coAgentId != null) {
            Set<String> listingsPerCoAgent = coAgents.get(coAgentId);
            if (listingsPerCoAgent == null) {
                listingsPerCoAgent = new HashSet<>();
                coAgents.put(coAgentId, listingsPerCoAgent);
            }
            listingsPerCoAgent.add(listingId);
            System.out.println("### listingsPerCoAgent " + listingsPerCoAgent);
        }


        jl.setListing(listing);


//        System.out.println();
//        System.out.println();
//        System.out.println("### current agent id: " + listing.getAgentId());
//        System.out.println("### listings for this agent: " + agents.get(listing.getAgentId()));
//        for (Iterator<Map.Entry<String, Set<String>>> it = agents.iterator(); it.hasNext(); ) {
//            Map.Entry<String, Set<String>> m = it.next();
//            System.out.println("### agent key: " + m.getKey() + " val: " + m.getValue());
//        }


//        System.out.println("### current coAgent id: " + listing.getCoListAgentId());
//        System.out.println("### listings for this coAgent: " + coAgents.get(listing.getCoListAgentId()));
//        for (Iterator<Map.Entry<String, Set<String>>> it = coAgents.iterator(); it.hasNext(); ) {
//            Map.Entry<String, Set<String>> m = it.next();
//            System.out.println("### coagent key: " + m.getKey() + " val: " + m.getValue());
//        }
//        System.out.println();
//        System.out.println();
        jls.add(jl);
        out.collect(jls);

    }

    @Override
    public void flatMap2(Agent agent, Collector<List<JoinedListing>> out) throws Exception {
        String agentId = agent.getAgentId();
        List<JoinedListing> jls = new ArrayList<>();
        System.out.println("### agent id " + agent.getAgentId());

        Set<String> listingsPerAgent = agents.get(agentId);
        System.out.println("### listingsPerAgent " + listingsPerAgent);
        if (listingsPerAgent != null) {
            for (String id : listingsPerAgent) {
                JoinedListing listi = joinedListings.get(id);
                if (listi != null) {
                    listi.setListAgent(agent);
                    jls.add(listi);
                }
            }
        }


//        Set<String> listingsPerCoAgent = coAgents.get(agentId);
//        System.out.println("### listingsPerCoAgent " + listingsPerCoAgent);
//        if (listingsPerCoAgent != null) {
//            for (String id : listingsPerCoAgent) {
//                JoinedListing listi = joinedListings.get(id);
//                if (listi != null) {
//                    listi.setCoListAgent(agent);
//                    jls.add(listi);
//                }
//            }
//        }
        System.out.println("### listings for this coAgent: " + coAgents.get(agentId));
        for (Iterator<Map.Entry<String, Set<String>>> it = coAgents.iterator(); it.hasNext(); ) {
            Map.Entry<String, Set<String>> m = it.next();
            System.out.println("### coagent key: " + m.getKey() + " val: " + m.getValue());
        }

        out.collect(jls);
    }
}
