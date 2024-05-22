package com.zxkw.userprofile.app;

import com.zxkw.userprofile.common.bean.TagInfo;
import com.zxkw.userprofile.common.constants.ConstCode;
import com.zxkw.userprofile.common.dao.TagInfoDao;
import com.zxkw.userprofile.common.util.ClickHouseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 优化查询性能:clickhouse标签宽表转换为bitmap表
 * //1. 取得全局标签
 * //2. 要把标签按照标签值类型，分成4份
 * //3. 写出插入bitmap表的静态方法
 * //4. 形成4个SQL,分别插入到不同的表中
 */
public class TaskBitMap {
    public static void main(String[] args) {
        String taskId = args[0];
        String businessDate = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("task_bitmap_app").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        //1. 取得全局标签
        List<TagInfo> tagInfoList = TagInfoDao.getTagInfoList();

        //2. 要把标签按照标签值类型，分成4份
        ArrayList<TagInfo> tagInfoStringList = new ArrayList<>();
        ArrayList<TagInfo> tagInfoLongList = new ArrayList<>();
        ArrayList<TagInfo> tagInfoDecimalList = new ArrayList<>();
        ArrayList<TagInfo> tagInfoDateList = new ArrayList<>();

        for (TagInfo tagInfo : tagInfoList) {
            if(ConstCode.TAG_VALUE_TYPE_STRING.equals(tagInfo.getTagValueType())){
                tagInfoStringList.add(tagInfo);
            }else if(ConstCode.TAG_VALUE_TYPE_LONG.equals(tagInfo.getTagValueType())){
                tagInfoLongList.add(tagInfo);
            }else if(ConstCode.TAG_VALUE_TYPE_DECIMAL.equals(tagInfo.getTagValueType())){
                tagInfoDecimalList.add(tagInfo);
            }else if(ConstCode.TAG_VALUE_TYPE_DATE.equals(tagInfo.getTagValueType())){
                tagInfoDateList.add(tagInfo);
            }
        }

        //3.形成4个SQL,分别插入到不同的表中
        insertBitmap(tagInfoStringList , "user_tag_value_string" , businessDate) ;
        insertBitmap(tagInfoLongList , "user_tag_value_long" , businessDate);
        insertBitmap(tagInfoDecimalList , "user_tag_value_decimal" , businessDate);
        insertBitmap(tagInfoDateList, "user_tag_value_date" , businessDate);

    }

    private static void insertBitmap(List<TagInfo> tagInfoList , String targetTableName , String businessDate){
        if(tagInfoList.size() > 0 ){
            //TODO 处理NULL值为0
            String tagCodeSQL = "" ;
            if (targetTableName.equals("user_tag_value_long") || targetTableName.equals("user_tag_value_decimal")) {
                tagCodeSQL = tagInfoList.stream().map(
                        tagInfo -> "('" + tagInfo.getTagCode().toLowerCase() + "', if(" + tagInfo.getTagCode().toLowerCase() + "='', '0', "+ tagInfo.getTagCode().toLowerCase() +"))"
                ).collect(Collectors.joining(","));
            }else{
                tagCodeSQL = tagInfoList.stream().map(
                        tagInfo -> "('" + tagInfo.getTagCode().toLowerCase() + "'," + tagInfo.getTagCode().toLowerCase() + ")"
                ).collect(Collectors.joining(","));
            }
            //幂等性处理
            String deleteSQL = " alter table " + targetTableName + " delete where dt = '" + businessDate + "'" ;
            ClickHouseUtil.executeSql(deleteSQL);

            String sourceTableName = "up_tag_merge_" + businessDate.replace("-","");

            String insertSQL = " insert into " + targetTableName +
                    " select tt.tg.1 as tag_code , tt.tg.2 as tag_value , groupBitmapState(cast (uid as UInt64)) , '" + businessDate + "'" +
                    " from (" +
                    " select uid , arrayJoin([ "+ tagCodeSQL + "]) tg " +
                    " from " + sourceTableName +
                    " ) tt" +
                    " group by tt.tg.1 , tt.tg.2" ;
            ClickHouseUtil.executeSql(insertSQL);

        }
    }
}
