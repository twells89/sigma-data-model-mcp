# Exercises two more "exotic" LookML constructs:
#   1. access_filter (explore-level row-level security via user_attribute) —
#      detected and warned (recreate as Sigma RLS), explore NOT dropped.
#   2. many_to_many relationship on a join — Sigma has no native M:N, so it is
#      mapped to the closest type (N:1) WITH a bridge-table warning, rather than
#      being silently dropped (which would lose the joined dimension columns).

view: order_fact {
  sql_table_name: CSA.TJ.ORDER_FACT ;;

  dimension: order_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.ORDER_ID ;;
  }
  dimension: promo_key {
    type: number
    sql: ${TABLE}.PROMO_KEY ;;
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
}

view: promo_dim {
  sql_table_name: CSA.TJ.PROMO_DIM ;;

  dimension: promo_key {
    primary_key: yes
    type: number
    sql: ${TABLE}.PROMO_KEY ;;
  }
  dimension: promo_name {
    type: string
    sql: ${TABLE}.PROMO_NAME ;;
  }
}

explore: order_fact {
  # Row-level security on ORDER_STATUS — no spec-level Sigma equivalent.
  access_filter: {
    field: order_fact.order_status
    user_attribute: allowed_status
  }

  # A many_to_many join (orders and promos relate many-to-many in this model).
  join: promo_dim {
    type: left_outer
    relationship: many_to_many
    sql_on: ${order_fact.promo_key} = ${promo_dim.promo_key} ;;
  }
}
