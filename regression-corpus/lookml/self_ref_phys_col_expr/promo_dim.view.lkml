view: promo_dim {
  sql_table_name: CSA.TJ.PROMO_DIM ;;

  dimension: promo_key {
    primary_key: yes
    sql: ${TABLE}.PROMO_KEY ;;
  }
  dimension: promo_name {
    sql: ${TABLE}.PROMO_NAME ;;
  }

  # REGRESSION: an EXPRESSION dimension whose name ("discount_pct" → "Discount Pct")
  # matches the physical column it references. The converter used to translate
  # ${TABLE}.DISCOUNT_PCT to the BARE display [Discount Pct], which self-collides
  # with this calc column's own output name → circular ref → type "error".
  # Correct output: [PROMO_DIM/Discount Pct] / 100.0
  dimension: discount_pct {
    type: number
    value_format_name: percent_1
    sql: ${TABLE}.DISCOUNT_PCT / 100.0 ;;
  }

  # REGRESSION: a legacy `case:` dimension referencing the same physical column.
  # Used to emit If([Discount Pct] >= 30, ...) (bare self-ref) → error.
  # Correct output: If([PROMO_DIM/Discount Pct] >= 30, ...)
  dimension: discount_band {
    label: "Discount Band"
    case: {
      when: {
        sql: ${TABLE}.DISCOUNT_PCT >= 30 ;;
        label: "Deep (30%+)"
      }
      when: {
        sql: ${TABLE}.DISCOUNT_PCT >= 15 ;;
        label: "Medium (15-29%)"
      }
      else: "Light (<15%)"
    }
  }
}
