import com.zxkw.userprofile.common.bean.TagInfo;
import com.zxkw.userprofile.common.constants.ConstCode;
import com.zxkw.userprofile.common.dao.TagInfoDao;
import com.zxkw.userprofile.common.util.ClickHouseUtil;
import com.zxkw.userprofile.common.util.MyPropsUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 为优化查询速度;成为即席查询,将合并好的宽表数据导出/迁移至clickhouse;如今估计流行doris
 * 1. 通过查询标签集合 , 获得字段名集合
 * 2. 表结构的平移,建一张clickhouse的表， 结构和hive的表字段一致
 * 3. 把数据写入clickhouse
 */
public class TaskExport {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("task_export_app").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        String taskId = args[0];
        String businessDate = args[1];

        String upDbName = MyPropsUtil.get(ConstCode.USER_PROFILE_DBNAME);
        String clickHouseUrl = MyPropsUtil.get(ConstCode.CLICKHOUSE_URL);

        //1. 通过查询标签集合 , 获得字段名集合
        List<TagInfo> tagInfoList = TagInfoDao.getTagInfoList();
        String fieldSQL =
                tagInfoList.stream().map(tagInfo -> tagInfo.getTagCode().toLowerCase() + " String").collect(Collectors.joining(","));

        //2. 表结构的平移,建一张clickhouse的表， 结构和hive的表字段一致
        // clickhouse的建表语句  表名  字段  引擎  主键  排序  分区  颗粒度8192
        // create table up_tag_merge_20200614
        //(
        //  uid String , 字段名 String ....
        // )
        // engine=MergeTree
        // order by uid
        String tableName = "up_tag_merge_"+businessDate.replace("-","");
        String dropTableSQL = " drop table if exists "+ upDbName+"."+tableName;
        System.out.println("dropTableSQL :" + dropTableSQL);

        ClickHouseUtil.executeSql(dropTableSQL);

        String createTableSQL = " create table if not exists " + upDbName + "." + tableName +
                " (" +
                " uid String , " + fieldSQL +
                " )" +
                " engine=MergeTree" +
                " order by uid";
        System.out.println("createTableSQL : "+ createTableSQL);
        ClickHouseUtil.executeSql(createTableSQL);

        //3. 把数据写入clickhouse
        // hiveTable  =>  dataFrame  =>  jdbc

        Dataset<Row> dataset = sparkSession.sql("select * from " + upDbName + "." + tableName);

        dataset.write().mode(SaveMode.Append)
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
                .option("batchsize" ,200)
                .option("isolationLevel", "NONE")
                .option("numPartitions",4)
                .jdbc(clickHouseUrl , tableName , new Properties());
    }
}
