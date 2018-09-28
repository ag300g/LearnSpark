# -*- coding: utf-8 -*-
__author__ = 'dz'
"""
    CREATE
        TABLE `app.app_wil_hot_sku_ratio_result`
        (
             `sku_id` string COMMENT 'sku编码',
             `dc_id` string COMMENT '配送中心编码',
             `hot_sku_target_store_id` string COMMENT '爆品仓编码',
             `future_source_store_id` string COMMENT '未来来源仓编码',
             `hot_sku_ratio` double COMMENT '爆品占比'
        )
        COMMENT '爆品占比结果' PARTITIONED BY
        (
            `dt` string,
            `pid` string,
            `ts` string
        )
        STORED AS ORC tblproperties
        (
            'orc.compress' = 'SNAPPY'
        )
"""
import sys
import datetime as dt
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
p = [["date", "输入数据的产生日期", "--date  输入数据的产生日期，选填，默认是今天，格式为yyyy-mm-dd", False, str(dt.date.today())],
     ["pid", "方案编号", "--pid  输入方案编号", False, None],
     ["ts", "方案时间戳", "--ts  输入方案时间戳", False, None],
     ["dc_id", "配送中心编号", "--dc_id  输入配送中心id", False, None]
     ]
def main():
    sc = SparkContext(conf=SparkConf().setAppName("wil_hot_sku_calc_online"))
    hc = HiveContext(sc)
    sc.addPyFile(sys.argv[1])
    from core.common import common
    param = common.init_params(sys.argv, p)
    date, pid, ts, dc_id = param["date"], param["pid"], param["ts"], param["dc_id"]
    today = dt.datetime.strptime(date, "%Y-%m-%d").date()
    yesterday = today - dt.timedelta(1)
    # 候选以及选入爆品仓的sku
    sql_sku_all = """
            select distinct t.sku_id as sku_id,
            t.current_source_store_id as store_id,
            t.future_source_store_id as future_store_id,
            case when t.b_sku_id is not null then 1 else 0 end as is_selected
            from
            (select a.sku_id as sku_id,
                   a.current_source_store_id as current_source_store_id,
                   a.future_source_store_id as future_source_store_id,
                   b.sku_id as b_sku_id
            from app.app_wil_hot_sku_all a
            left join
            app.app_wil_hot_sku_selected b
            on a.sku_id = b.sku_id
            ) t
    """
    hc.sql(sql_sku_all).createOrReplaceTempView("tb_sku_all")

    # 所有的候选sku关联的订单数据
    sql_data_all = """
        select a.sku_id as sku_id,
               a.future_store_id as future_store_id,
               a.is_selected as is_selected,
               b.out_wh_tm as out_wh_tm,
               b.ord_id as ord_id,
               b.sale_qtty as sale_qtty
        from
            (select sku_id,
                     store_id,
                    future_store_id,
                    is_selected
            from tb_sku_all
            ) a
        left join
            (select sku_id,
                   store_id,
                   out_wh_tm,
                   parent_sale_ord_id as ord_id,
                   sale_qtty
            from app.app_wil_hot_sale_store_sku_ord_sale
            where dt = '""" + str(yesterday) + """'
                and sale_qtty >= 0
            ) b
        on a.store_id = b.store_id
            and a.sku_id = b.sku_id
    """
    hc.sql(sql_data_all).createOrReplaceTempView("tb_data_all")
    # 标注订单
    sql_choice_ord = """
        select a.ord_id,
               case when (a.f1=1 and a.f2=1) or (a.f1=2 and a.f2=1) or (a.f1=2 and a.f2=2 and (a.g1=2 or (a.g2=2 and a.g3=1))) then 1 else 0 end as choice_flag2
        from
            (select ord_id,
                count(distinct future_store_id) as f1,
                count(distinct is_selected) as f2,
                count(distinct future_store_id,is_selected) as g1,
                count(distinct (case when is_selected = 1 then future_store_id else null end)) as g2,
                count(distinct (case when is_selected = 0 then future_store_id else null end)) as g3
            from tb_data_all
            group by ord_id
            ) a
    """
    hc.sql(sql_choice_ord).createOrReplaceTempView("tb_choice_ord")

    # 统计销量
    sql_result = """
        select a.sku_id,
               sum(case when b.choice_flag2 = 1 then a.sale_qtty else 0 end) as sale_in_hot,
               sum(a.sale_qtty) as sale_total
        from tb_data_all a
        left join
            tb_choice_ord b
        on a.ord_id = b.ord_id
        where a.is_selected = 1
        group by a.sku_id
    """
    hc.sql(sql_result).createOrReplaceTempView("tb_result")

    # 默认的分配比例
    sql_selected_sku_info = """
        select a.sku_id as sku_id,
               b.dc_id as dc_id,
               b.hot_sku_target_store_id as hot_sku_target_store_id,
               b.future_source_store_id as future_source_store_id,
               b.default_ratio as default_ratio
        from app.app_wil_hot_sku_selected a
        left join 
            (select sku_id,
                    dc_id,
                    hot_sku_target_store_id,
                    future_source_store_id,
                    avg(hot_sku_out_store_rate) as default_ratio
            from app.app_wil_hot_sku_all
            group by sku_id,dc_id,hot_sku_target_store_id,future_source_store_id
            ) b
        on a.sku_id = b.sku_id
    """
    hc.sql(sql_selected_sku_info).createOrReplaceTempView("tb_selected_sku_info")

    # 生成最终的爆品比例结果
    sql_final_result = """
            select
                a.sku_id,
                a.dc_id,
                a.hot_sku_target_store_id,
                a.future_source_store_id,
                coalesce(round(b.sale_in_hot/b.sale_total,4),a.default_ratio) as hot_sku_ratio
            from tb_selected_sku_info a
            left join tb_result b
            on a.sku_id = b.sku_id
    """
    hc.sql(sql_final_result).createOrReplaceTempView("tb_final_result")

    # 把相关的结果整合到app_wil_hot_sku_ratio_result表，以dt,pid,ts为分区
    partition = "dt='" + str(param["date"]) + "', pid='" + str(param["pid"]) + "', ts='" + str(param["ts"]) + "'"
    table_output = 'app.app_wil_hot_sku_ratio_result'
    sql_save_sd_result = """
                insert overwrite table """ + table_output + """ partition (""" + partition + """)
                select
                    sku_id,
                    dc_id,
                    hot_sku_target_store_id,
                    future_source_store_id,
                    hot_sku_ratio
                from tb_final_result
            """
    hc.sql(sql_save_sd_result)

if __name__ == "__main__":
    main()
