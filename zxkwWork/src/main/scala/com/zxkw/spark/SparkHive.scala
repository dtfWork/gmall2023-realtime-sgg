import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    // 获取带有hive支持的SparkSession
    val sparkSession=SparkSession.builder().appName("hive on spark")
      .master("local").enableHiveSupport().getOrCreate()
    val sc=sparkSession.sparkContext
    // 创建数据库
    sparkSession.sql("create database if not exists sparkstudy")
    // 进入数据库
    sparkSession.sql("use sparkstudy")
    // 创建hive表
    sparkSession.sql("create table if not exists student(id int,name string,score int) " +
      "row format delimited fields terminated by ','")
    // 加载数据
    sparkSession.sql("load data local inpath './datas/student.csv' " +
      "overwrite into table student")
    // 查询数据
    sparkSession.sql("select * from student").show()

    sc.stop()
    sparkSession.close()
  }
}