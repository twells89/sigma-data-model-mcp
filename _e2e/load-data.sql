-- Grain-exercising synthetic data. Shared domain: roles {AE,SDR} x countries
-- {United States,Canada} x weeks {2025-01-06 cw1, 2025-01-13 cw2}, qtr 'Q1-25'.
-- product_goals has 2 PRODUCTS per (role,week) and special_projects 2 INITIATIVES
-- per role => deliberate many-to-many to verify the pre-agg JOIN does NOT fan out.
TRUNCATE TABLE REDACTED_DB.STATIC.PRESALES_CW_LEVEL;
TRUNCATE TABLE REDACTED_DB.STATIC.PRESALE_WEEKLY_GOALS;
TRUNCATE TABLE REDACTED_DB.STATIC.ACTIVATIONS_GOALS_PRESALES;
TRUNCATE TABLE REDACTED_DB.STATIC.PRESALE_PRODUCT_GOALS;
TRUNCATE TABLE REDACTED_DB.STATIC.PRESALE_SPECIAL_PROJECTS_GOALS;
TRUNCATE TABLE REDACTED_DB.PUBLIC.SALES_ANALYTICS_BIS;

-- FACT: 3 opportunity rows per (role,country,week) => 2*2*2*3 = 24 rows.
INSERT INTO REDACTED_DB.STATIC.PRESALES_CW_LEVEL
  (CW_OWNER_ROLE_OFFICIAL_TITLE, OWNER_ROLE_OFFICIAL_TITLE, CW_COUNTRY, COUNTRY, CLEAN_COUNTRY,
   WEEK_OF, CW_WEEK, QTR, YR, QTR_YR, STORE_ID, STORE_ACCOUNT_ID, MERCHANT_ACTIVATION_DATE,
   MERCHANT_CLOSE_DATE, INCREMENTAL_SIGN_TAM, NARR, DECK_RANK, TOP_MX, MANAGER_NAME, SALES_OWNER, REGION)
SELECT role, role, country, country, country, wk, cwk, 'Q1', 2025, 'Q1-25',
       seq8(), 'ACC-'||seq8()::string, dateadd('day', 2, wk), wk,
       1000*(uniform(1,5,random())), 50*(uniform(1,9,random())),
       decode(uniform(1,3,random()),1,'Ace',2,'King','Jack'),
       (uniform(0,1,random())=1), 'Mgr '||role, role||' Rep '||seq8()::string, country
FROM (SELECT column1 role FROM values('AE'),('SDR')) r,
     (SELECT column1 country FROM values('United States'),('Canada')) c,
     (SELECT column1 wk, column2 cwk FROM values('2025-01-06'::date,1),('2025-01-13'::date,2)) w,
     TABLE(generator(rowcount => 3));

-- WEEKLY_GOALS: one row per (role,country,cw_week,qtr_yr,week_of) => m:1
INSERT INTO REDACTED_DB.STATIC.PRESALE_WEEKLY_GOALS (QTR,YR,QTR_YR,COUNTRY,ROLE,CLOSES,NARR,CW_WEEK,WORKING_DAYS,AHC_GOAL,CLOSE_PHASING,WEEK_OF)
SELECT 'Q1',2025,'Q1-25',country,role, 20, 1000, cwk, 5, 8, 0.25, wk
FROM (SELECT column1 role FROM values('AE'),('SDR')) r,
     (SELECT column1 country FROM values('United States'),('Canada')) c,
     (SELECT column1 wk, column2 cwk FROM values('2025-01-06'::date,1),('2025-01-13'::date,2)) w;

-- ACTIVATIONS_GOALS: one per (role,country) => m:1
INSERT INTO REDACTED_DB.STATIC.ACTIVATIONS_GOALS_PRESALES (WEEK_GOAL_ACTIVATIONS,ROLE_ACTIVATIONS,ACTIVATION_GOAL,QUARTER_YEAR,COUNTRY)
SELECT '2025-01-06'::date, role, 15, 'Q1-25', country
FROM (SELECT column1 role FROM values('AE'),('SDR')) r,
     (SELECT column1 country FROM values('United States'),('Canada')) c;

-- PRODUCT_GOALS: 2 PRODUCTS per (role, week) => m:m on (role,week). WEEK_GOAL is TEXT MM/DD/YYYY.
INSERT INTO REDACTED_DB.STATIC.PRESALE_PRODUCT_GOALS (BASED,ORG,PRODUCT,PRODUCT_GOAL,QTR_YR_PG,ROLE,TARGET,TEAM,WEEK_GOAL)
SELECT 'US','PreSales',product, 10, 'Q1-25', role, 'T1','Team A', wk
FROM (SELECT column1 role FROM values('AE'),('SDR')) r,
     (SELECT column1 product FROM values('Ads'),('DashPass')) p,
     (SELECT column1 wk FROM values('01/06/2025'),('01/13/2025')) w;

-- SPECIAL_PROJECTS: 2 INITIATIVES per role => m:m on role.
INSERT INTO REDACTED_DB.STATIC.PRESALE_SPECIAL_PROJECTS_GOALS (WEEK_GOAL_SP,INITIATIVE,INITIATIVE_GOAL,QTR_YR_SP,ROLE_SP)
SELECT '2025-01-06'::date, initiative, 5, 'Q1-25', role
FROM (SELECT column1 role FROM values('AE'),('SDR')) r,
     (SELECT column1 initiative FROM values('T4-5 Activations'),('Whales')) i;

-- SALES_ANALYTICS_BIS (active_hc source): a few reps per role. rep_segment maps to country.
INSERT INTO REDACTED_DB.PUBLIC.SALES_ANALYTICS_BIS (REP_SEGMENT,DAYS_OFF,DAYS_WORKED,WORKING_DAYS,ROLE_TITLE,WEEK,REP_NAME,DDMX_ID)
SELECT seg, 1, 4, 5, role, '2025-01-06', role||' '||seq8()::string, seq8()::string
FROM (SELECT column1 role FROM values('AE'),('SDR')) r,
     (SELECT column1 seg FROM values('US SMB Sales'),('CAN SMB Sales')) s,
     TABLE(generator(rowcount => 2));

SELECT 'fact' t, COUNT(*) n FROM REDACTED_DB.STATIC.PRESALES_CW_LEVEL
UNION ALL SELECT 'weekly', COUNT(*) FROM REDACTED_DB.STATIC.PRESALE_WEEKLY_GOALS
UNION ALL SELECT 'product', COUNT(*) FROM REDACTED_DB.STATIC.PRESALE_PRODUCT_GOALS
UNION ALL SELECT 'special', COUNT(*) FROM REDACTED_DB.STATIC.PRESALE_SPECIAL_PROJECTS_GOALS
UNION ALL SELECT 'activations', COUNT(*) FROM REDACTED_DB.STATIC.ACTIVATIONS_GOALS_PRESALES
UNION ALL SELECT 'bis', COUNT(*) FROM REDACTED_DB.PUBLIC.SALES_ANALYTICS_BIS;
