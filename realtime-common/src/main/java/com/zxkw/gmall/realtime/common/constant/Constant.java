package com.zxkw.gmall.realtime.common.constant;

/**
 * @author lenovo
 * 常量配置类:包含数据库连接配置，kafka-topic配置等
 */
public class Constant {
    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String HBASE_NAMESPACE = "gmall";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start_log";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err_log";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page_log";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action_log";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display_log";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_pay_suc_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_pay_suc_detail";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String DORIS_FE_NODES = "hadoop102:7030";

    public static final String DORIS_DATABASE = "gmall2023_realtime";

    public static final int TWO_DAY_SECONDS = 2 * 24 * 60 * 60;

}
