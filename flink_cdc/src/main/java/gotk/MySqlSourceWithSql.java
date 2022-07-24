package gotk;

import com.sun.rowset.internal.Row;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author HuangKai
 * @date 2022/7/23 0:14
 */
public class MySqlSourceWithSql {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "delopy");

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.DDL方式建表
        tableEnv.executeSql("CREATE TABLE mysql_binlog ( " +
                " id STRING NOT NULL, " +
                " tm_name STRING, " +
                " logo_url STRING, " +
                "  PRIMARY KEY(id) NOT ENFORCED "+
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '192.168.50.109', " +
                " 'port' = '3307', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'gmall_flink', " +
                " 'table-name' = 'base_trademark' " +
                ")");

        //3.查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        //4.将动态表转换为流
        DataStream<Tuple2<Boolean, com.sun.rowset.internal.Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        //5.启动任务
        env.execute("FlinkCDCWithSQL");
    }
}
