package com.atguigu.edu.publisher.mapper;

import com.atguigu.edu.publisher.bean.UserChangeCtPerType;
import com.atguigu.edu.publisher.bean.UserPageCt;
import com.atguigu.edu.publisher.bean.UserTradeCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface UserStatsMapper {
    @Select("select 'back_count'          type,\n" +
            "       sum(back_count)    back_count\n" +
            "from dws_user_login_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select 'activeUserCt'     type,\n" +
            "       sum(uv_count)      uv_count\n" +
            "from dws_user_login_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select 'newUserCt' type,\n" +
            "       sum(register_count) register_count\n" +
            "from dws_user_register_window\n" +
//            "where toYYYYMMDD(stt) = #{date}" +
            ";")
    List<UserChangeCtPerType> selectUserChangeCtPerType(@Param("date")Integer date);

    @Select("select page_id,\n" +
            "       uvCt\n" +
            "from\n" +
            "(select 'home'  page_id,\n" +
            "       sum(home_uv_count)        uvCt\n" +
            "       from dws_traffic_page_view_window\n" +
//            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'list_uv' page_id,\n" +
            "       sum(list_uv_count) uvCt\n" +
            "       from dws_traffic_page_view_window\n" +
//            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'detail_uv' page_id,\n" +
            "       sum(detail_uv_count) uvCt\n" +
            "       from dws_traffic_page_view_window\n" +
//            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'cart' page_id,\n" +
            "       sum(cart_add_uv_count) uvCt\n" +
            "       from dws_trade_cart_add_window\n" +
//            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'trade' page_id,\n" +
            "       sum(order_uv_count) uvCt\n" +
            "       from dws_trade_order_window\n" +
//            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'payment' page_id,\n" +
            "       sum(pay_suc_uv_count) uvCt\n" +
            "       from dws_trade_pay_suc_window\n" +
//            "       where toYYYYMMDD(stt) = #{date}\n" +
            ") all_data order by uvCt desc;")
    List<UserPageCt> selectUvByPage(@Param("date") Integer date);

    @Select("select 'order' trade_type,\n" +
            "       sum(new_order_user_count) new_order_user_count\n" +
            "       from dws_trade_order_window\n" +
            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'payment' trade_type,\n" +
            "       sum(pay_suc_new_user_count) pay_suc_new_user_count\n" +
            "       from dws_trade_pay_suc_window\n" +
            "       where toYYYYMMDD(stt) = #{date};")
    List<UserTradeCt> selectTradeUserCt(@Param("date")Integer date);

}
