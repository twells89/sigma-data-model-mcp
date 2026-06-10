view: order_fact {
  sql_table_name: CSA.TJ.ORDER_FACT ;;

  dimension: order_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.ORDER_ID ;;
  }
  dimension: order_status {
    type: string
    sql: ${TABLE}.ORDER_STATUS ;;
  }
  dimension: net_revenue {
    type: number
    sql: ${TABLE}.NET_REVENUE ;;
  }
  dimension: gross_profit {
    type: number
    sql: ${TABLE}.GROSS_PROFIT ;;
  }
  dimension: days_to_ship {
    type: number
    sql: ${TABLE}.DAYS_TO_SHIP ;;
  }

  # ── base aggregate measures the ratios reference ──
  measure: total_net_revenue {
    type: sum
    sql: ${net_revenue} ;;
  }
  measure: total_gross_profit {
    type: sum
    sql: ${gross_profit} ;;
  }
  measure: order_count {
    type: count
  }

  # ── BUG3: ratio measures referencing OTHER measures ──
  # avg_order_value = revenue / order_count — exercises a measure-over-measure
  # ratio. Pre-fix the ${measure} tokens leaked into the formula and fabricated
  # phantom ${...} columns → type:error.
  measure: avg_order_value {
    type: number
    sql: ${total_net_revenue} / ${order_count} ;;
    value_format_name: usd
  }
  # gross_margin_pct exercises BOTH a `1.0 *` numeric literal (forces float
  # division) AND a NULLIF(...) guard against divide-by-zero. NULLIF must map to
  # Sigma's NullIf(); the literal and operators pass through untouched.
  measure: gross_margin_pct {
    type: number
    sql: 1.0 * ${total_gross_profit} / NULLIF(${total_net_revenue}, 0) ;;
    value_format_name: percent_2
  }

  # ── BUG5: percentile measure ──
  # p90_days_to_ship → Percentile([Days to Ship], 0.9)
  measure: p90_days_to_ship {
    type: percentile
    percentile: 90
    sql: ${days_to_ship} ;;
  }

  # ── BUG6: filtered `type: count` with filters: and NO sql: ──
  # delivered_order_count → CountIf([Order Status] = "Delivered"). Pre-fix this
  # fabricated a phantom DELIVERED_ORDER_COUNT physical column → type:error.
  measure: delivered_order_count {
    type: count
    filters: [order_status: "Delivered"]
  }
}

explore: order_fact {}
