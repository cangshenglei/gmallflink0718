package com.atguigu.app.dws;


import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordBean;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//数据流:app->ngnix>日志服务器(File)>Flume>>>>kafak>>>>>FlinkApp-----kafka(DWD)---FlinkAPP---clickhouse
//程序:Mock_log ->Flume()->kafka->BaseLogApp->Kafka(ZK)->DwsTrafficSourceKeywordPageViewWindow----clickHouse
//需求:从KAFAK页面浏览主题数据,过滤搜素行为,使用是定义UDTF(一进多出),对搜索内容分词
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        //TODO  1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //TODO  2,使用DDL方式读取KAFKA页面日志主题数据创建表,同时提取事件时间用来生成WaterMark
        tableEnv.executeSql("" +
                "create table page_view( " +
                "    `page` Map<String,String>, " +
                "    `ts` Bigint, " +
                "    `rt` AS TO_TIMESTAMP_LTZ(ts,3), " +//配置函数
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ") " + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "keyword_220212"));
        //TODO  3,过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] words, " +
                "    rt " +
                "from page_view " +
                "where page['item_type']='keyword' " +
                "and page['last_page_id']='search' " +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);
        //TODO  4,注册函数并切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    rt, " +
                "    word " +
                "FROM filter_table, LATERAL TABLE(SplitFunction(words))");
        tableEnv.createTemporaryView("split_table", splitTable);

        //TODO  5,分组,开窗,聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +//窗口开启时间
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +//窗口关闭时间
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP() ts " +//转换时间戳函数
                "from split_table " +
                "group by word, " +
                "TUMBLE(rt, INTERVAL '10' SECOND)");
        //TODO  6,将动态表转为流
        DataStream<KeywordBean> keywordDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordDataStream.print(">>>>>>>>>");

        //TODO  7,将数据写入到clickhouse
        keywordDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        ///TODO 8,启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }


}
