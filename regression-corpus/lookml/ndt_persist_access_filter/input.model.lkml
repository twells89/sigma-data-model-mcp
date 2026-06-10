# Exercises the Native Derived Table + PDT-persistence constructs:
#   1. Native Derived Table (derived_table with explore_source) — translated to
#      a Custom SQL element that aggregates the base table (SUM/COUNT + GROUP BY).
#   2. PDT persistence hints (datagroup_trigger / persist_for / sql_trigger_value)
#      — mapped to an informational Sigma scheduled-materialization note.
# Converted explore = status_summary (see convertOptions.exploreName).

view: order_fact {
  sql_table_name: CSA.TJ.ORDER_FACT ;;

  dimension: order_id {
    primary_key: yes
    type: number
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

  measure: total_net_revenue {
    type: sum
    sql: ${net_revenue} ;;
  }
  measure: order_count {
    type: count
  }
}

explore: order_fact {}

# ── Native Derived Table: aggregates the order_fact explore ──
# No raw sql: — aggregation is declared via explore_source + column blocks.
# Persistence hints should map to Sigma scheduled materialization.
view: status_summary {
  derived_table: {
    explore_source: order_fact {
      column: order_status {
        field: order_fact.order_status
      }
      column: total_net_revenue {
        field: order_fact.total_net_revenue
      }
      column: order_count {
        field: order_fact.order_count
      }
    }
    datagroup_trigger: nightly_etl
    persist_for: "24 hours"
    sql_trigger_value: SELECT CURRENT_DATE ;;
  }

  dimension: order_status {
    type: string
    sql: ${TABLE}.order_status ;;
  }
  dimension: total_net_revenue {
    type: number
    sql: ${TABLE}.total_net_revenue ;;
  }
  dimension: order_count {
    type: number
    sql: ${TABLE}.order_count ;;
  }
}

explore: status_summary {}
