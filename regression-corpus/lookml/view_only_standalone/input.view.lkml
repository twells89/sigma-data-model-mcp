# Synthesized view-only fixture: a bare .view.lkml with NO model/explore file —
# the raw "customer sent me view files" scenario. The converter must not throw;
# it converts the view as a standalone Custom SQL element.
view: channel_revenue {
  derived_table: {
    sql:
      SELECT ORDER_CHANNEL,
             ORDER_STATUS,
             SUM(NET_REVENUE) AS NET_REVENUE,
             COUNT(DISTINCT ORDER_ID) AS NUM_ORDERS
      FROM CSA.TJ.ORDER_FACT
      GROUP BY 1,2 ;;
  }

  dimension: order_channel {
    type: string
    sql: ${TABLE}."ORDER_CHANNEL" ;;
  }

  dimension: order_status {
    type: string
    sql: ${TABLE}."ORDER_STATUS" ;;
  }

  dimension: net_revenue {
    type: number
    sql: ${TABLE}."NET_REVENUE" ;;
  }

  measure: total_net_revenue {
    type: sum
    value_format_name: usd_0
    sql: ${net_revenue} ;;
  }

  measure: num_orders_sum {
    type: sum
    sql: ${TABLE}."NUM_ORDERS" ;;
  }
}
