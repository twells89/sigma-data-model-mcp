create or replace semantic view WORKFORCE_ANOMALY_VIEW
	tables (
		CSA.TJ.EMPLOYEES primary key (EMPLOYEE_ID) comment='Employee roster with role, compensation, and status.',
		CSA.TJ.TIME_ENTRIES primary key (ENTRY_ID) comment='Daily time tracking: clock in/out, hours, overtime, break compliance.',
		CSA.TJ.ABSENCE_RECORDS primary key (ABSENCE_ID) comment='Employee absences: sick, PTO, personal leave.',
		CSA.TJ.SAFETY_INCIDENTS primary key (INCIDENT_ID) comment='Workplace safety incidents and near-misses.',
		CSA.TJ.LABOR_COSTS primary key (COST_ID) comment='Monthly labor costs by location and department.'
	)
	relationships (
		TIME_ENTRIES_TO_EMPLOYEES as TIME_ENTRIES(EMPLOYEE_ID) references EMPLOYEES(EMPLOYEE_ID),
		ABSENCE_RECORDS_TO_EMPLOYEES as ABSENCE_RECORDS(EMPLOYEE_ID) references EMPLOYEES(EMPLOYEE_ID),
		SAFETY_INCIDENTS_TO_EMPLOYEES as SAFETY_INCIDENTS(EMPLOYEE_ID) references EMPLOYEES(EMPLOYEE_ID)
	)
	facts (
		EMPLOYEES.ANNUAL_SALARY as ANNUAL_SALARY with synonyms=('salary') comment='Annual base salary USD',
		TIME_ENTRIES.SCHEDULED_HOURS as SCHEDULED_HOURS comment='Scheduled hours',
		TIME_ENTRIES.ACTUAL_HOURS as ACTUAL_HOURS with synonyms=('hours worked') comment='Actual hours worked',
		TIME_ENTRIES.OVERTIME_HOURS as OVERTIME_HOURS with synonyms=('OT hours') comment='Overtime hours',
		ABSENCE_RECORDS.HOURS as HOURS comment='Hours absent (8=full, 4=half)',
		SAFETY_INCIDENTS.HOURS_WORKED_BEFORE_INCIDENT as HOURS_WORKED_BEFORE_INCIDENT comment='Hours worked before incident',
		LABOR_COSTS.HEADCOUNT as HEADCOUNT comment='Monthly headcount',
		LABOR_COSTS.BASE_PAYROLL as BASE_PAYROLL with synonyms=('payroll') comment='Base payroll USD',
		LABOR_COSTS.OVERTIME_COST as OVERTIME_COST comment='Overtime cost USD',
		LABOR_COSTS.TEMP_WORKER_COST as TEMP_WORKER_COST comment='Temp worker cost USD',
		LABOR_COSTS.BENEFITS_COST as BENEFITS_COST comment='Benefits cost USD',
		LABOR_COSTS.TOTAL_LABOR_COST as TOTAL_LABOR_COST comment='Total labor cost USD'
	)
	dimensions (
		EMPLOYEES.EMPLOYEE_ID as EMPLOYEE_ID with synonyms=('emp id') comment='Unique employee identifier',
		EMPLOYEES.EMPLOYEE_NAME as FIRST_NAME || ' ' || LAST_NAME with synonyms=('name') comment='Full name',
		EMPLOYEES.DEPARTMENT as DEPARTMENT with synonyms=('dept') comment='Department name',
		EMPLOYEES.ROLE as ROLE with synonyms=('job title') comment='Job title',
		EMPLOYEES.LOCATION as LOCATION with synonyms=('site') comment='Work location',
		EMPLOYEES.STATUS as STATUS comment='Active, Terminated, or Resigned',
		EMPLOYEES.TERMINATION_REASON as TERMINATION_REASON comment='Reason for leaving',
		EMPLOYEES.MANAGER_ID as MANAGER_ID with synonyms=('manager') comment='Manager employee ID',
		EMPLOYEES.EMPLOYMENT_TYPE as EMPLOYMENT_TYPE comment='Full-Time or Part-Time',
		EMPLOYEES.SHIFT as SHIFT comment='Day, Swing, or Night',
		EMPLOYEES.HIRE_DATE as HIRE_DATE comment='Date hired',
		EMPLOYEES.TERMINATION_DATE as TERMINATION_DATE comment='Date terminated or resigned',
		TIME_ENTRIES.ENTRY_ID as ENTRY_ID comment='Time entry ID',
		TIME_ENTRIES.EMPLOYEE_ID as EMPLOYEE_ID comment='Employee ID',
		TIME_ENTRIES.LOCATION as LOCATION comment='Work location',
		TIME_ENTRIES.DEPARTMENT as DEPARTMENT comment='Department',
		TIME_ENTRIES.BREAK_TAKEN as BREAK_TAKEN comment='Whether break was taken',
		TIME_ENTRIES.DATE as DATE with synonyms=('work date') comment='Date of time entry',
		ABSENCE_RECORDS.ABSENCE_ID as ABSENCE_ID comment='Absence record ID',
		ABSENCE_RECORDS.EMPLOYEE_ID as EMPLOYEE_ID comment='Employee ID',
		ABSENCE_RECORDS.ABSENCE_TYPE as ABSENCE_TYPE comment='Sick, PTO, or Personal',
		ABSENCE_RECORDS.DAY_OF_WEEK as DAY_OF_WEEK comment='Day of week',
		ABSENCE_RECORDS.APPROVED as APPROVED comment='Whether approved',
		ABSENCE_RECORDS.DEPARTMENT as DEPARTMENT comment='Department',
		ABSENCE_RECORDS.LOCATION as LOCATION comment='Location',
		ABSENCE_RECORDS.DATE as DATE comment='Absence date',
		SAFETY_INCIDENTS.INCIDENT_ID as INCIDENT_ID comment='Incident ID',
		SAFETY_INCIDENTS.EMPLOYEE_ID as EMPLOYEE_ID comment='Employee ID',
		SAFETY_INCIDENTS.LOCATION as LOCATION comment='Incident location',
		SAFETY_INCIDENTS.DEPARTMENT as DEPARTMENT comment='Department',
		SAFETY_INCIDENTS.INCIDENT_TYPE as INCIDENT_TYPE comment='Type of incident',
		SAFETY_INCIDENTS.SEVERITY as SEVERITY comment='Low, Medium, High, or Critical',
		SAFETY_INCIDENTS.OVERTIME_ON_DAY as OVERTIME_ON_DAY comment='Whether employee was on overtime',
		SAFETY_INCIDENTS.DATE as DATE comment='Incident date',
		LABOR_COSTS.COST_ID as COST_ID comment='Cost record ID',
		LABOR_COSTS.LOCATION as LOCATION comment='Location',
		LABOR_COSTS.DEPARTMENT as DEPARTMENT comment='Department',
		LABOR_COSTS.MONTH as MONTH with synonyms=('period') comment='Month of cost record'
	)
	metrics (
		EMPLOYEES.HEADCOUNT as COUNT(EMPLOYEE_ID) comment='Total employees',
		EMPLOYEES.ACTIVE_HEADCOUNT as COUNT_IF(STATUS = 'Active') comment='Active employees',
		EMPLOYEES.AVERAGE_SALARY as AVG(ANNUAL_SALARY) comment='Average annual salary',
		TIME_ENTRIES.TOTAL_HOURS_WORKED as SUM(ACTUAL_HOURS) comment='Sum of actual hours',
		TIME_ENTRIES.TOTAL_OVERTIME_HOURS as SUM(OVERTIME_HOURS) comment='Sum of overtime hours',
		TIME_ENTRIES.AVG_DAILY_HOURS as AVG(ACTUAL_HOURS) comment='Average hours per day',
		TIME_ENTRIES.BREAK_COMPLIANCE_RATE as AVG(CASE WHEN BREAK_TAKEN THEN 1.0 ELSE 0.0 END) * 100 comment='Percent of shifts with breaks taken',
		TIME_ENTRIES.SHIFT_COUNT as COUNT(ENTRY_ID) comment='Number of shifts',
		ABSENCE_RECORDS.TOTAL_ABSENCES as COUNT(ABSENCE_ID) comment='Total absence count',
		ABSENCE_RECORDS.SICK_DAY_COUNT as COUNT_IF(ABSENCE_TYPE = 'Sick') comment='Sick absence count',
		SAFETY_INCIDENTS.INCIDENT_COUNT as COUNT(INCIDENT_ID) comment='Total incidents',
		SAFETY_INCIDENTS.CRITICAL_INCIDENT_COUNT as COUNT_IF(SEVERITY IN ('High', 'Critical')) comment='High or Critical incidents',
		LABOR_COSTS.TOTAL_LABOR_SPEND as SUM(TOTAL_LABOR_COST) comment='Sum of total labor costs',
		LABOR_COSTS.TOTAL_OVERTIME_SPEND as SUM(OVERTIME_COST) comment='Sum of overtime costs'
	)
	comment='Workforce analytics for Acme Corp: 300 employees, 5 locations, 6 departments, Jan 2024 - Mar 2026.'
	with extension (CA='{"tables":[{"name":"EMPLOYEES","dimensions":[{"name":"EMPLOYEE_ID"},{"name":"EMPLOYEE_NAME"},{"name":"DEPARTMENT"},{"name":"ROLE"},{"name":"LOCATION"},{"name":"STATUS"},{"name":"TERMINATION_REASON"},{"name":"MANAGER_ID"},{"name":"EMPLOYMENT_TYPE"},{"name":"SHIFT"}],"facts":[{"name":"ANNUAL_SALARY"}],"metrics":[{"name":"HEADCOUNT"},{"name":"ACTIVE_HEADCOUNT"},{"name":"AVERAGE_SALARY"}],"time_dimensions":[{"name":"HIRE_DATE"},{"name":"TERMINATION_DATE"}]},{"name":"TIME_ENTRIES","dimensions":[{"name":"ENTRY_ID"},{"name":"EMPLOYEE_ID"},{"name":"LOCATION"},{"name":"DEPARTMENT"},{"name":"BREAK_TAKEN"}],"facts":[{"name":"SCHEDULED_HOURS"},{"name":"ACTUAL_HOURS"},{"name":"OVERTIME_HOURS"}],"metrics":[{"name":"TOTAL_HOURS_WORKED"},{"name":"TOTAL_OVERTIME_HOURS"},{"name":"AVG_DAILY_HOURS"},{"name":"BREAK_COMPLIANCE_RATE"},{"name":"SHIFT_COUNT"}],"time_dimensions":[{"name":"DATE"}]},{"name":"ABSENCE_RECORDS","dimensions":[{"name":"ABSENCE_ID"},{"name":"EMPLOYEE_ID"},{"name":"ABSENCE_TYPE"},{"name":"DAY_OF_WEEK"},{"name":"APPROVED"},{"name":"DEPARTMENT"},{"name":"LOCATION"}],"facts":[{"name":"HOURS"}],"metrics":[{"name":"TOTAL_ABSENCES"},{"name":"SICK_DAY_COUNT"}],"time_dimensions":[{"name":"DATE"}]},{"name":"SAFETY_INCIDENTS","dimensions":[{"name":"INCIDENT_ID"},{"name":"EMPLOYEE_ID"},{"name":"LOCATION"},{"name":"DEPARTMENT"},{"name":"INCIDENT_TYPE"},{"name":"SEVERITY"},{"name":"OVERTIME_ON_DAY"}],"facts":[{"name":"HOURS_WORKED_BEFORE_INCIDENT"}],"metrics":[{"name":"INCIDENT_COUNT"},{"name":"CRITICAL_INCIDENT_COUNT"}],"time_dimensions":[{"name":"DATE"}]},{"name":"LABOR_COSTS","dimensions":[{"name":"COST_ID"},{"name":"LOCATION"},{"name":"DEPARTMENT"}],"facts":[{"name":"HEADCOUNT"},{"name":"BASE_PAYROLL"},{"name":"OVERTIME_COST"},{"name":"TEMP_WORKER_COST"},{"name":"BENEFITS_COST"},{"name":"TOTAL_LABOR_COST"}],"metrics":[{"name":"TOTAL_LABOR_SPEND"},{"name":"TOTAL_OVERTIME_SPEND"}],"time_dimensions":[{"name":"MONTH"}]}],"relationships":[{"name":"TIME_ENTRIES_TO_EMPLOYEES","relationship_type":"many_to_one"},{"name":"ABSENCE_RECORDS_TO_EMPLOYEES","relationship_type":"many_to_one"},{"name":"SAFETY_INCIDENTS_TO_EMPLOYEES","relationship_type":"many_to_one"}],"verified_queries":[{"name":"overtime_by_location_baseline_vs_anomaly","sql":"SELECT LOCATION, ROUND(SUM(CASE WHEN DATE < ''2026-01-01'' THEN OVERTIME_HOURS ELSE 0 END) / NULLIF(COUNT(CASE WHEN DATE < ''2026-01-01'' THEN 1 END), 0), 2) AS BASELINE_AVG_OT, ROUND(SUM(CASE WHEN DATE >= ''2026-01-01'' THEN OVERTIME_HOURS ELSE 0 END) / NULLIF(COUNT(CASE WHEN DATE >= ''2026-01-01'' THEN 1 END), 0), 2) AS ANOMALY_AVG_OT, ROUND(SUM(CASE WHEN DATE >= ''2026-01-01'' THEN OVERTIME_HOURS ELSE 0 END), 1) AS TOTAL_OT_2026 FROM CSA.TJ.TIME_ENTRIES GROUP BY LOCATION ORDER BY TOTAL_OT_2026 DESC","question":"Compare overtime by location baseline vs anomaly period"},{"name":"break_compliance_by_location","sql":"SELECT LOCATION, COUNT(*) AS TOTAL_SHIFTS, ROUND(COUNT_IF(BREAK_TAKEN = TRUE) * 100.0 / COUNT(*), 1) AS BREAK_COMPLIANCE_PCT FROM CSA.TJ.TIME_ENTRIES WHERE DATE >= ''2026-01-01'' GROUP BY LOCATION ORDER BY BREAK_COMPLIANCE_PCT ASC","question":"What is the break compliance rate by location?"},{"name":"safety_incidents_by_location_period","sql":"SELECT LOCATION, COUNT_IF(DATE < ''2026-01-01'') AS BASELINE_INCIDENTS, COUNT_IF(DATE >= ''2026-01-01'') AS ANOMALY_INCIDENTS, COUNT_IF(DATE >= ''2026-01-01'' AND SEVERITY IN (''High'', ''Critical'')) AS HIGH_SEV_2026, ROUND(AVG(CASE WHEN DATE >= ''2026-01-01'' THEN HOURS_WORKED_BEFORE_INCIDENT END), 1) AS AVG_HRS_BEFORE_2026, COUNT_IF(DATE >= ''2026-01-01'' AND OVERTIME_ON_DAY = TRUE) AS OT_INCIDENTS_2026 FROM CSA.TJ.SAFETY_INCIDENTS GROUP BY LOCATION ORDER BY ANOMALY_INCIDENTS DESC","question":"How many safety incidents by location in baseline vs anomaly period?"},{"name":"labor_cost_ot_ratio_by_location","sql":"SELECT LOCATION, ROUND(SUM(OVERTIME_COST), 0) AS TOTAL_OT_COST, ROUND(SUM(BASE_PAYROLL), 0) AS TOTAL_BASE, ROUND(SUM(OVERTIME_COST) / NULLIF(SUM(BASE_PAYROLL), 0) * 100, 1) AS OT_PCT_OF_PAYROLL FROM CSA.TJ.LABOR_COSTS WHERE MONTH >= ''2026-01-01'' GROUP BY LOCATION ORDER BY OT_PCT_OF_PAYROLL DESC","question":"What is overtime cost as a percentage of base payroll by location?"},{"name":"absence_patterns_by_day_dept","sql":"SELECT DEPARTMENT, COUNT(*) AS TOTAL_ABSENCES, ROUND(AVG(CASE WHEN DAY_OF_WEEK IN (''Monday'', ''Friday'', ''Mon'', ''Fri'') THEN 1.0 ELSE 0.0 END) * 100, 1) AS MON_FRI_PCT, ROUND(COUNT_IF(ABSENCE_TYPE = ''Sick'') * 100.0 / COUNT(*), 1) AS SICK_PCT FROM CSA.TJ.ABSENCE_RECORDS WHERE DATE >= ''2026-01-01'' GROUP BY DEPARTMENT ORDER BY MON_FRI_PCT DESC","question":"Show absence patterns by day of week and department"},{"name":"manager_turnover","sql":"SELECT e2.FIRST_NAME || '' '' || e2.LAST_NAME AS MANAGER_NAME, e2.LOCATION, e2.DEPARTMENT, COUNT(*) AS TOTAL_REPORTS, COUNT_IF(e1.STATUS IN (''Terminated'', ''Resigned'')) AS SEPARATED, ROUND(COUNT_IF(e1.STATUS IN (''Terminated'', ''Resigned'')) * 100.0 / COUNT(*), 1) AS TURNOVER_PCT FROM CSA.TJ.EMPLOYEES e1 JOIN CSA.TJ.EMPLOYEES e2 ON e1.MANAGER_ID = e2.EMPLOYEE_ID GROUP BY e2.EMPLOYEE_ID, e2.FIRST_NAME, e2.LAST_NAME, e2.LOCATION, e2.DEPARTMENT HAVING COUNT(*) >= 3 ORDER BY TURNOVER_PCT DESC","question":"Which managers have the highest turnover?"},{"name":"avg_daily_hours_by_location","sql":"SELECT LOCATION, ROUND(AVG(ACTUAL_HOURS), 2) AS AVG_DAILY_HOURS FROM CSA.TJ.TIME_ENTRIES WHERE DATE >= ''2026-01-01'' GROUP BY LOCATION ORDER BY AVG_DAILY_HOURS DESC","question":"What is the average daily hours worked by location?"},{"name":"chicago_overtime_safety_correlation","sql":"SELECT COUNT(*) AS TOTAL_INCIDENTS, COUNT_IF(OVERTIME_ON_DAY = TRUE) AS OT_INCIDENTS, ROUND(AVG(HOURS_WORKED_BEFORE_INCIDENT), 1) AS AVG_HOURS_BEFORE, COUNT_IF(SEVERITY IN (''High'', ''Critical'')) AS HIGH_SEVERITY FROM CSA.TJ.SAFETY_INCIDENTS WHERE LOCATION = ''Chicago Plant'' AND DATE >= ''2026-01-01''","question":"How do safety incidents correlate with overtime at Chicago Plant?"},{"name":"employees_over_50hrs_weekly","sql":"SELECT t.EMPLOYEE_ID, e.FIRST_NAME || '' '' || e.LAST_NAME AS NAME, e.DEPARTMENT, e.LOCATION, DATE_TRUNC(''WEEK'', t.DATE) AS WEEK_START, ROUND(SUM(t.ACTUAL_HOURS), 1) AS WEEKLY_HOURS FROM CSA.TJ.TIME_ENTRIES t JOIN CSA.TJ.EMPLOYEES e ON t.EMPLOYEE_ID = e.EMPLOYEE_ID WHERE t.DATE >= ''2026-01-01'' GROUP BY t.EMPLOYEE_ID, e.FIRST_NAME, e.LAST_NAME, e.DEPARTMENT, e.LOCATION, WEEK_START HAVING SUM(t.ACTUAL_HOURS) > 50 ORDER BY WEEKLY_HOURS DESC","question":"Which employees work more than 50 hours per week?"},{"name":"monthly_labor_cost_trend","sql":"SELECT MONTH, LOCATION, ROUND(SUM(TOTAL_LABOR_COST), 0) AS TOTAL_COST FROM CSA.TJ.LABOR_COSTS GROUP BY MONTH, LOCATION ORDER BY MONTH, TOTAL_COST DESC","question":"Show monthly labor cost trend by location"},{"name":"chicago_labor_before_after","sql":"SELECT CASE WHEN MONTH < ''2026-01-01'' THEN ''Baseline (Jul-Dec 2025)'' ELSE ''Anomaly (Jan-Mar 2026)'' END AS PERIOD, ROUND(AVG(BASE_PAYROLL), 0) AS AVG_BASE, ROUND(AVG(OVERTIME_COST), 0) AS AVG_OT, ROUND(AVG(TEMP_WORKER_COST), 0) AS AVG_TEMP, ROUND(AVG(TOTAL_LABOR_COST), 0) AS AVG_TOTAL FROM CSA.TJ.LABOR_COSTS WHERE LOCATION = ''Chicago Plant'' GROUP BY PERIOD ORDER BY PERIOD","question":"Compare Chicago Plant labor costs before and after January 2026"},{"name":"headcount_by_department_location","sql":"SELECT DEPARTMENT, LOCATION, COUNT(*) AS HEADCOUNT, COUNT_IF(STATUS = ''Active'') AS ACTIVE FROM CSA.TJ.EMPLOYEES GROUP BY DEPARTMENT, LOCATION ORDER BY HEADCOUNT DESC","question":"Show headcount by department and location"},{"name":"long_shifts_by_location","sql":"SELECT LOCATION, COUNT(*) AS LONG_SHIFTS FROM CSA.TJ.TIME_ENTRIES WHERE ACTUAL_HOURS > 10 AND DATE >= ''2026-01-01'' GROUP BY LOCATION ORDER BY LONG_SHIFTS DESC","question":"How many shifts exceed 10 hours by location?"},{"name":"absence_rate_by_location","sql":"SELECT a.LOCATION, COUNT(DISTINCT a.EMPLOYEE_ID) AS EMPLOYEES_WITH_ABSENCES, COUNT(*) AS TOTAL_ABSENCES, ROUND(SUM(a.HOURS), 0) AS TOTAL_HOURS_ABSENT FROM CSA.TJ.ABSENCE_RECORDS a WHERE a.DATE >= ''2026-01-01'' GROUP BY a.LOCATION ORDER BY TOTAL_ABSENCES DESC","question":"What is the absence rate by location?"},{"name":"incident_types_breakdown","sql":"SELECT INCIDENT_TYPE, COUNT(*) AS COUNT, COUNT_IF(SEVERITY IN (''High'', ''Critical'')) AS HIGH_CRITICAL FROM CSA.TJ.SAFETY_INCIDENTS GROUP BY INCIDENT_TYPE ORDER BY COUNT DESC","question":"What types of safety incidents are most common?"}]}');
