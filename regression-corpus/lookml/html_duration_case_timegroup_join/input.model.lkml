view: order_fact {
  sql_table_name: CSA.TJ.ORDER_FACT ;;

  dimension: order_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.ORDER_ID ;;
  }
  dimension: promo_key {
    type: number
    sql: ${TABLE}.PROMO_KEY ;;
  }
  dimension: net_revenue {
    type: number
    sql: ${TABLE}.NET_REVENUE ;;
  }
  dimension: days_to_ship {
    type: number
    sql: ${TABLE}.DAYS_TO_SHIP ;;
  }

  # ── BUG4: html: block with Liquid {% if %} FOLLOWED BY more fields ──
  # An html: parameter containing a Liquid conditional used to desync the parser
  # (it swallowed the closing braces / the value column) so EVERY dimension and
  # measure declared AFTER it silently disappeared. The dimensions and measures
  # below this block prove they survive the html parse.
  dimension: order_status {
    type: string
    sql: ${TABLE}.ORDER_STATUS ;;
    html:
      {% if value == 'Delivered' %}
        <span style="color: green">{{ value }}</span>
      {% else %}
        <span style="color: red">{{ value }}</span>
      {% endif %} ;;
  }

  # These MUST still appear after the html/Liquid block above (BUG4).
  dimension: ship_method {
    type: string
    sql: ${TABLE}.SHIP_METHOD ;;
  }

  # ── BUG8: legacy `case: { when / else }` dimension ──
  dimension: ship_speed_bucket {
    case: {
      when: {
        sql: ${TABLE}.DAYS_TO_SHIP <= 2 ;;
        label: "Fast"
      }
      when: {
        sql: ${TABLE}.DAYS_TO_SHIP <= 5 ;;
        label: "Standard"
      }
      else: "Slow"
    }
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
  dimension: channel {
    type: string
    sql: ${TABLE}.CHANNEL ;;
  }

  # ── BUG9: dimension_group type: time on the joined dim ──
  # The denormalized/derived element built off order_fact must surface this
  # group's DateTrunc timeframe columns (Promo Start Month/Quarter/Year, etc.)
  # cross-element. Verified in regression by the timeframe columns existing.
  dimension_group: promo_start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.START_DATE ;;
  }

  # ── BUG7: dimension_group type: duration with sql_start / sql_end ──
  dimension_group: promo_run {
    type: duration
    intervals: [day, week]
    sql_start: ${TABLE}.START_DATE ;;
    sql_end: ${TABLE}.END_DATE ;;
  }
}

explore: order_fact {
  join: promo_dim {
    type: left_outer
    relationship: many_to_one
    sql_on: ${order_fact.promo_key} = ${promo_dim.promo_key} ;;
  }
}
