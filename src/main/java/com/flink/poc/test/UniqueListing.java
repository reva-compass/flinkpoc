package com.flink.poc.test;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class UniqueListing extends RichFlatMapFunction<Listing, Listing> {

    private transient ValueState<Listing> sum;

    @Override
    public void flatMap(Listing input, Collector<Listing> out) throws Exception {

        // access the state value
      //  Listing currentSum = sum.value();
//        System.out.println("### listing id" + input.getListingId());
//
//        System.out.println("### sum before " + sum.value());

        sum.update(input);
     //   System.out.println("### sum after" + sum.value());

        out.collect(input);
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Listing> descriptor =
                new ValueStateDescriptor<>(
                        "uniqueListingState", // the state name
                        TypeInformation.of(new TypeHint<Listing>() {}), // type information
                        new Listing()); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}
