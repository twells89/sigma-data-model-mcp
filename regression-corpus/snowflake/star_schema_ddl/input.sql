CREATE OR REPLACE SEMANTIC VIEW ANALYTICS.MODELS.SALES_VIEW
  TABLES (
    lineitem AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
      COMMENT = 'Order lines, one row per part shipped',
    orders AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
      PRIMARY KEY (o_orderkey)
      WITH SYNONYMS = ('sales orders', 'purchase orders')
      COMMENT = 'One row per order',
    customer AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
      PRIMARY KEY (c_custkey)
      UNIQUE (c_name)
  )
  RELATIONSHIPS (
    lines_to_orders AS lineitem (l_orderkey) REFERENCES orders (o_orderkey),
    orders_to_customer AS orders (o_custkey) REFERENCES customer (c_custkey)
  )
  FACTS (
    PRIVATE lineitem.net_price AS l_extendedprice * (1 - l_discount)
      COMMENT = 'Discounted line price, don''t use directly',
    lineitem.l_quantity AS l_quantity,
    customer.order_count AS COUNT(orders.o_orderkey)
  )
  DIMENSIONS (
    orders.o_orderstatus LABELS = (FILTER) AS o_orderstatus
      WITH SYNONYMS = ('status'),
    customer.c_name AS c_name,
    orders.customer_name AS customer.c_name,
    orders.order_month AS DATE_TRUNC(month, o_orderdate)
  )
  METRICS (
    lineitem.total_revenue AS SUM(net_price)
      COMMENT = 'Revenue net of discount',
    lineitem.average_quantity AS AVG(l_quantity),
    lineitem.distinct_orders AS COUNT(DISTINCT l_orderkey)
  )
  COMMENT = 'Sales star schema (lineitem, orders, customer)'
  MAX_STALENESS = 3600
  COPY GRANTS;
