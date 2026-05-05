view: customer_dim {
  sql_table_name: CSA.TJ.CUSTOMER_DIM ;;

  dimension: customer_key {
    primary_key: yes
    type: number
    sql: ${TABLE}.CUSTOMER_KEY ;;
  }

  dimension: customer_segment {
    type: string
    sql: ${TABLE}.CUSTOMER_SEGMENT ;;
  }
}

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

  dimension: gross_revenue {
    type: number
    sql: ${TABLE}.GROSS_REVENUE ;;
  }

  # Cross-element calc col: references customer_dim.customer_segment
  dimension: customer_segment_label {
    type: string
    sql: CONCAT('Segment: ', ${customer_dim.customer_segment}) ;;
  }
}

explore: order_fact {
  join: customer_dim {
    type: left_outer
    relationship: many_to_one
    sql_on: ${order_fact.customer_key} = ${customer_dim.customer_key} ;;
  }
}
