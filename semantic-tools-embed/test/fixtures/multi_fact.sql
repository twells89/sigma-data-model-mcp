CREATE OR REPLACE SEMANTIC VIEW ANALYTICS.CLAIMS.UTILIZATION_SV
  TABLES (
    admissions AS ANALYTICS.CLAIMS.ADMISSIONS
      PRIMARY KEY (admission_id)
      COMMENT = 'One row per inpatient admission',
    member_months AS ANALYTICS.CLAIMS.MEMBER_MONTHS
      PRIMARY KEY (member_month_id)
      COMMENT = 'One row per member per month of coverage',
    dim_age_band AS ANALYTICS.CLAIMS.DIM_AGE_BAND
      PRIMARY KEY (age_band_key),
    dim_region AS ANALYTICS.CLAIMS.DIM_REGION
      PRIMARY KEY (region_key),
    dim_plan AS ANALYTICS.CLAIMS.DIM_PLAN
      PRIMARY KEY (plan_key),
    dim_facility AS ANALYTICS.CLAIMS.DIM_FACILITY
      PRIMARY KEY (facility_key)
  )
  RELATIONSHIPS (
    adm_to_age AS admissions (age_band_key) REFERENCES dim_age_band (age_band_key),
    adm_to_region AS admissions (region_key) REFERENCES dim_region (region_key),
    adm_to_plan AS admissions (plan_key) REFERENCES dim_plan (plan_key),
    adm_to_facility AS admissions (facility_key) REFERENCES dim_facility (facility_key),
    mm_to_age AS member_months (age_band_key) REFERENCES dim_age_band (age_band_key),
    mm_to_region AS member_months (region_key) REFERENCES dim_region (region_key),
    mm_to_plan AS member_months (plan_key) REFERENCES dim_plan (plan_key)
  )
  DIMENSIONS (
    dim_age_band.age_band AS age_band,
    dim_region.region_name AS region_name,
    dim_plan.plan_name AS plan_name,
    dim_facility.facility_name AS facility_name
  )
  METRICS (
    admissions.admits AS SUM(admissions.paid_admit_cnt),
    member_months.member_months_total AS SUM(member_months.mm),
    admits_ptmpy AS SUM(admissions.paid_admit_cnt) / NULLIF(SUM(member_months.mm), 0) * 12000
  );
