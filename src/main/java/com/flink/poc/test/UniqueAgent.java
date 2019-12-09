package com.flink.poc.test;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class UniqueAgent extends RichFlatMapFunction<Agent, Agent>  {
    private transient ValueState<Agent> sum;

    @Override
    public void flatMap(Agent input, Collector<Agent> out) throws Exception {

        // access the state value
//        System.out.println("### agent id" + input.getAgentId());
//
//        System.out.println("### sum before " + sum.value());

        sum.update(input);
   //     System.out.println("### sum after" + sum.value());

        out.collect(input);
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Agent> descriptor =
                new ValueStateDescriptor<>(
                        "uniqueAgentState", // the state name
                        TypeInformation.of(new TypeHint<Agent>() {}), // type information
                        new Agent()); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}
