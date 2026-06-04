view: order_fact {
  sql_table_name: CSA.TJ.ORDER_FACT ;;

  dimension: order_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.ORDER_ID ;;
  }
  dimension: customer_key {
    type: number
    sql: ${TABLE}.CUSTOMER_KEY ;;
  }
  dimension: order_status {
    type: string
    sql: ${TABLE}.ORDER_STATUS ;;
  }
  dimension: net_revenue {
    type: number
    sql: ${TABLE}.NET_REVENUE ;;
  }
  dimension: unit_price {
    type: number
    sql: ${TABLE}.UNIT_PRICE ;;
  }

  # Bug 1: measures reference sibling dimensions via ${dim}. Pre-fix these leaked
  # literal ${...} tokens into formulas (Sum([${net revenue}])) and fabricated
  # phantom ${...} columns → type:error.
  measure: total_net_revenue {
    type: sum
    sql: ${net_revenue} ;;
  }
  measure: avg_unit_price {
    type: average
    sql: ${unit_price} ;;
  }
  measure: distinct_customers {
    type: count_distinct
    sql: ${customer_key} ;;
  }
  measure: delivered_revenue {
    type: sum
    sql: ${net_revenue} ;;
    filters: [order_status: "Delivered"]
  }
}

view: customer_dim {
  sql_table_name: CSA.TJ.CUSTOMER_DIM ;;

  dimension: customer_key {
    primary_key: yes
    type: number
    sql: ${TABLE}.CUSTOMER_KEY ;;
  }
  dimension: region {
    type: string
    sql: ${TABLE}.REGION ;;
  }
}

view: store_dim {
  sql_table_name: CSA.TJ.STORE_DIM ;;

  dimension: store_key {
    primary_key: yes
    type: number
    sql: ${TABLE}.STORE_KEY ;;
  }
  dimension: store_name {
    type: string
    sql: ${TABLE}.STORE_NAME ;;
  }
}

# Bug 2: store_dim's FK lives on customer_dim, not the base (order_fact). Pre-fix
# the converter wired EVERY join off the base element with base.PK = target.key,
# so this relationship became order_fact.order_id = store_dim.store_key (text=int)
# → type:error. Fixed: the relationship attaches to the customer_dim element.
# customer_key=store_key keeps the join-key types matched (both numbers) so a
# correctly-wired model has zero error columns.
explore: order_fact {
  join: customer_dim {
    type: left_outer
    sql_on: ${order_fact.customer_key} = ${customer_dim.customer_key} ;;
    relationship: many_to_one
  }
  join: store_dim {
    type: left_outer
    sql_on: ${customer_dim.customer_key} = ${store_dim.store_key} ;;
    relationship: many_to_one
  }
}
