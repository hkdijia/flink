package com.gotk;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.50.109")
                .port(3307)
                .databaseList("gmall_flink") // set captured database
                .tableList("gmall_flink.base_attr_info") // set captured table
                .username("root")
                .password("123456")
                .deserializer(new CustomerDeserialization()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.latest())
                .build();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("FlinkCDCWithCustomerDeserialization");
    }
}