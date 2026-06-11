# Synthesized layered-LookML fixture (generic trades/rates names; structural
# patterns from a customer trial: CTE-continuation fragment derived SQL,
# cross-view ${view.SQL_TABLE_NAME} ref to another derived view, incremental
# PDT ({% incrementcondition %} + increment_key), dimension_groups with CAST
# and timeframe lists without raw, and a MAX(${dim_group_raw}) date_time
# measure). Points at CSA.TJ.ORDER_FACT so the live corpus gate can query it.
connection: "snowflake"

explore: trades_enriched {}

view: daily_rates {
  derived_table: {
    sql:
      SELECT TO_DATE(TO_VARCHAR(ORDER_DATE_KEY), 'YYYYMMDD') AS PRICE_DATE,
             ORDER_CHANNEL AS ASSET,
             AVG(UNIT_PRICE) AS USD_RATE
      FROM CSA.TJ.ORDER_FACT
      GROUP BY 1,2 ;;
    datagroup_trigger: nightly_datagroup
  }

  dimension: asset {
    type: string
    sql: ${TABLE}."ASSET" ;;
  }
}

view: trades_enriched {
  derived_table: {
    sql:
      -- CTE-continuation fragment: Looker prepends inlined PDT CTEs before this comma
      , base AS (
          SELECT t.ORDER_ID,
                 TO_DATE(TO_VARCHAR(t.ORDER_DATE_KEY), 'YYYYMMDD') AS ORDER_DATE,
                 t.NET_REVENUE,
                 t.ORDER_CHANNEL,
                 REGEXP_REPLACE(t.ORDER_STATUS, '\\d+', '') AS STATUS_CLEAN
          FROM CSA.TJ.ORDER_FACT t
          WHERE {% incrementcondition %} t.ORDER_DATE_KEY {% endincrementcondition %}
      )
      SELECT b.*, r.USD_RATE
      FROM base b
      LEFT JOIN ${daily_rates.SQL_TABLE_NAME} r
        ON b.ORDER_DATE = r.PRICE_DATE AND b.ORDER_CHANNEL = r.ASSET ;;

    datagroup_trigger: nightly_datagroup
    increment_key: "order_date"
    increment_offset: 2
    cluster_keys: ["ORDER_DATE"]
  }

  dimension: order_id {
    type: string
    sql: ${TABLE}."ORDER_ID" ;;
  }

  dimension: order_channel {
    type: string
    sql: ${TABLE}."ORDER_CHANNEL" ;;
  }

  dimension: status_clean {
    type: string
    sql: ${TABLE}."STATUS_CLEAN" ;;
  }

  dimension: net_revenue {
    type: number
    sql: ${TABLE}."NET_REVENUE" ;;
  }

  dimension: usd_rate {
    type: number
    sql: ${TABLE}."USD_RATE" ;;
  }

  dimension_group: order_date {
    type: time
    timeframes: [date, week, month]
    sql: CAST(${TABLE}."ORDER_DATE" AS TIMESTAMP_NTZ) ;;
  }

  dimension_group: max_seen_dim {
    hidden: yes
    type: time
    timeframes: [raw]
    sql: ${TABLE}."ORDER_DATE" ;;
  }

  measure: total_net_revenue {
    type: sum
    value_format_name: usd_0
    sql: ${net_revenue} ;;
  }

  measure: max_seen {
    type: date_time
    convert_tz: no
    sql: MAX(${max_seen_dim_raw}) ;;
  }

  measure: count {
    type: count
  }
}
