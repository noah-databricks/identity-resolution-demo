-- Post-generation realism gate. Each statement returns one metrics row and raises an
-- error (failing the job task) when a population statistic leaves its accepted band.

-- COMMAND ----------
-- Criterion 1: name diversity (canonical population).
WITH names AS (
  SELECT count(*) people,count(DISTINCT given_name) given_names,count(DISTINCT family_name) family_names,
    count(DISTINCT concat(given_name,' ',family_name)) full_names
  FROM {{catalog}}.{{truth_schema}}.canonical_people
), top_full AS (
  SELECT max(n) max_full_name_holders FROM (
    SELECT count(*) n FROM {{catalog}}.{{truth_schema}}.canonical_people GROUP BY given_name,family_name)
)
SELECT 'names' gate,people,given_names,family_names,full_names,round(full_names/people,4) full_name_ratio,max_full_name_holders,
  IF(given_names>=1000 AND family_names>=5000 AND full_names>=0.80*people AND max_full_name_holders<=25,'PASS',
    raise_error(concat('name diversity out of band: given=',given_names,' family=',family_names,
      ' full=',full_names,' max_holders=',max_full_name_holders))) result
FROM names CROSS JOIN top_full;

-- COMMAND ----------
-- Criterion 3: every canonical locality is a real (suburb, state, postcode) triple; spread and mix.
WITH located AS (
  SELECT p.home_country,p.location_class,p.home_suburb,p.home_state,p.home_postcode,
    a.locality_key IS NOT NULL au_valid,o.overseas_key IS NOT NULL overseas_valid
  FROM {{catalog}}.{{truth_schema}}.canonical_people p
  LEFT JOIN {{catalog}}.{{reference_schema}}.au_localities a
    ON p.home_country='AU' AND a.suburb=p.home_suburb AND a.state=p.home_state AND a.postcode=p.home_postcode
  LEFT JOIN {{catalog}}.{{reference_schema}}.overseas_localities o
    ON p.home_country<>'AU' AND o.country=p.home_country AND o.suburb=p.home_suburb AND o.region=p.home_state
      AND coalesce(nullif(o.postcode,''),'-')=coalesce(p.home_postcode,'-')
), stats AS (
  SELECT count(*) people,
    count(DISTINCT IF(home_country='AU',concat(home_suburb,'|',home_state),NULL)) au_suburbs,
    count_if(home_country='AU' AND NOT au_valid) invalid_au_triples,
    count_if(home_country<>'AU' AND NOT overseas_valid) invalid_overseas,
    count_if(location_class='greater_sydney')/count(*) greater_sydney_share,
    count_if(location_class='other_au')/count(*) other_au_share,
    count_if(location_class='overseas')/count(*) overseas_share
  FROM located
)
SELECT 'locality' gate,*,
  IF(au_suburbs>=300 AND invalid_au_triples=0 AND invalid_overseas=0
      AND greater_sydney_share BETWEEN 0.70 AND 0.80 AND other_au_share BETWEEN 0.12 AND 0.18
      AND overseas_share BETWEEN 0.08 AND 0.12,'PASS',
    raise_error(concat('locality realism out of band: suburbs=',au_suburbs,' invalid_au=',invalid_au_triples,
      ' invalid_overseas=',invalid_overseas,' sydney=',greater_sydney_share,' other_au=',other_au_share,
      ' overseas=',overseas_share))) result
FROM stats;

-- COMMAND ----------
-- Criterion 4: personal email domains, local-part numbering and uniqueness.
WITH emails AS (
  SELECT personal_email,split(personal_email,'@')[1] domain,split(personal_email,'@')[0] local_part,
    floor(months_between(DATE '2025-12-31',date_of_birth)/12) age
  FROM {{catalog}}.{{truth_schema}}.canonical_people
), stats AS (
  SELECT count(*) people,count(DISTINCT personal_email) distinct_emails,
    count_if(domain='gmail.com')/count(*) gmail,
    count_if(domain IN ('outlook.com','outlook.com.au','hotmail.com','hotmail.com.au','hotmail.co.uk','live.com','live.com.au'))/count(*) microsoft,
    count_if(domain IN ('icloud.com','me.com'))/count(*) icloud,
    count_if(domain IN ('yahoo.com','yahoo.com.au','ymail.com','y7mail.com'))/count(*) yahoo,
    count_if(domain IN ('bigpond.com','bigpond.net.au','optusnet.com.au','iinet.net.au','tpg.com.au'))/count(*) au_isp,
    count_if(domain IN ('bigpond.com','bigpond.net.au','optusnet.com.au','iinet.net.au','tpg.com.au') AND age>=55)
      /greatest(count_if(age>=55),1) au_isp_55_plus,
    count_if(domain IN ('bigpond.com','bigpond.net.au','optusnet.com.au','iinet.net.au','tpg.com.au') AND age<35)
      /greatest(count_if(age<35),1) au_isp_under_35,
    count_if(local_part RLIKE '[0-9]')/count(*) numbered
  FROM emails
)
SELECT 'personal_email' gate,*,
  IF(distinct_emails=people AND gmail BETWEEN 0.40 AND 0.50 AND microsoft BETWEEN 0.18 AND 0.25
      AND icloud BETWEEN 0.10 AND 0.15 AND yahoo BETWEEN 0.04 AND 0.07 AND au_isp BETWEEN 0.05 AND 0.10
      AND au_isp_55_plus>2*au_isp_under_35 AND numbered<=0.35,'PASS',
    raise_error(concat('personal email realism out of band: gmail=',gmail,' microsoft=',microsoft,' icloud=',icloud,
      ' yahoo=',yahoo,' isp=',au_isp,' numbered=',numbered,' distinct=',distinct_emails))) result
FROM stats;

-- COMMAND ----------
-- Criterion 5 (canonical): employers, work email and company always travel together and agree.
WITH work AS (
  SELECT p.*,e.domain employer_domain,
    regexp_replace(lower(p.given_name),'[^a-z]','') gtok,regexp_replace(lower(p.family_name),'[^a-z]','') ftok
  FROM {{catalog}}.{{truth_schema}}.canonical_people p
  LEFT JOIN {{catalog}}.{{reference_schema}}.employers e ON e.company_name=p.company_name
), stats AS (
  SELECT count(DISTINCT company_name) employers_with_staff,
    max(IF(company_name IS NOT NULL,staff,NULL)) largest_employer,min(IF(company_name IS NOT NULL,staff,NULL)) smallest_employer,
    sum(violation_email_without_company) email_without_company,
    sum(violation_company_without_email) company_without_email,
    sum(violation_domain) domain_mismatch,sum(violation_local) local_part_mismatch,
    count(work_email)-count(DISTINCT work_email) duplicate_work_emails
  FROM (
    SELECT *,count(*) OVER (PARTITION BY company_name) staff,
      IF(work_email IS NOT NULL AND company_name IS NULL,1,0) violation_email_without_company,
      IF(company_name IS NOT NULL AND work_email IS NULL,1,0) violation_company_without_email,
      IF(work_email IS NOT NULL AND split(work_email,'@')[1]<>employer_domain,1,0) violation_domain,
      IF(work_email IS NOT NULL AND instr(split(work_email,'@')[0],ftok)=0 AND instr(split(work_email,'@')[0],gtok)=0,1,0) violation_local
    FROM work)
)
SELECT 'work_identity_canonical' gate,*,
  IF(employers_with_staff>=200 AND largest_employer>=20*smallest_employer AND email_without_company=0
      AND company_without_email=0 AND domain_mismatch=0 AND local_part_mismatch=0 AND duplicate_work_emails=0,'PASS',
    raise_error(concat('canonical work identity out of band: employers=',employers_with_staff,
      ' email_without_company=',email_without_company,' company_without_email=',company_without_email,
      ' domain_mismatch=',domain_mismatch,' local_mismatch=',local_part_mismatch))) result
FROM stats;

-- COMMAND ----------
-- Criterion 5 (source-visible): a work email is only shown with its company, on the company's domain.
-- Documented source-field gap: payments cards carry a company name but never a business email.
WITH visible AS (
  SELECT r.source_system,r.company_name,r.work_email,e.domain employer_domain
  FROM {{catalog}}.{{source_schema}}.source_identity_records r
  LEFT JOIN {{catalog}}.{{reference_schema}}.employers e
    ON lower(e.company_name)=lower(regexp_replace(r.company_name,' Pty Ltd$',''))
), stats AS (
  SELECT count_if(work_email IS NOT NULL AND company_name IS NULL) email_without_company,
    count_if(company_name IS NOT NULL AND work_email IS NULL AND source_system<>'payments') company_without_email_outside_gap,
    count_if(company_name IS NOT NULL AND work_email IS NULL AND source_system='payments') payments_company_only,
    count_if(work_email IS NOT NULL AND (employer_domain IS NULL OR split(work_email,'@')[1]<>employer_domain)) domain_mismatch,
    count_if(work_email IS NOT NULL) work_emails
  FROM visible
)
SELECT 'work_identity_visible' gate,*,
  IF(email_without_company=0 AND company_without_email_outside_gap=0 AND domain_mismatch=0,'PASS',
    raise_error(concat('visible work identity out of band: email_without_company=',email_without_company,
      ' company_without_email=',company_without_email_outside_gap,' domain_mismatch=',domain_mismatch))) result
FROM stats;

-- COMMAND ----------
-- Criterion 6: records per guest are heavy tailed (generator v4.1), sources stay balanced,
-- regulars concentrate in at most three systems and overseas visitors rarely return.
WITH per_person AS (
  SELECT t.truth_person_id,count(*) n,count(DISTINCT t.source_system) sources,max(g.visit_segment) visit_segment
  FROM {{catalog}}.{{truth_schema}}.identity_record_truth t
  JOIN {{catalog}}.{{truth_schema}}.guest_relationships g USING (truth_person_id)
  GROUP BY t.truth_person_id
), by_source AS (
  SELECT min(c) min_source_records,max(c) max_source_records,count(*) source_systems
  FROM (SELECT source_system,count(*) c FROM {{catalog}}.{{source_schema}}.source_identity_records GROUP BY source_system)
), stats AS (
  SELECT (SELECT count(*) FROM {{catalog}}.{{truth_schema}}.canonical_people) people,
    count(*) people_with_records,sum(n) records,min(n) min_records,max(n) max_records,
    round(count_if(n=1)/count(*),4) share_1,
    round(count_if(n BETWEEN 2 AND 3)/count(*),4) share_2_3,
    round(count_if(n BETWEEN 4 AND 8)/count(*),4) share_4_8,
    round(count_if(n BETWEEN 9 AND 20)/count(*),4) share_9_20,
    round(count_if(n BETWEEN 21 AND 60)/count(*),4) share_21_60,
    count_if(n>=9 AND sources>3) regulars_over_3_sources,
    round(count_if(visit_segment='overseas_visitor' AND n<=2)/greatest(count_if(visit_segment='overseas_visitor'),1),4) overseas_1_2,
    round(avg(IF(visit_segment IN ('venue_local','hospitality_worker'),n,NULL))/avg(IF(visit_segment IN ('sydney_resident','interstate_or_regional'),n,NULL)),3) local_depth_lift
  FROM per_person
)
SELECT 'records_per_person' gate,s.*,b.*,
  IF(people_with_records=people AND min_records>=1 AND max_records<=60
      AND share_1 BETWEEN 0.55 AND 0.65 AND share_2_3 BETWEEN 0.20 AND 0.25 AND share_4_8 BETWEEN 0.08 AND 0.12
      AND share_9_20 BETWEEN 0.03 AND 0.06 AND share_21_60 BETWEEN 0.005 AND 0.015
      AND regulars_over_3_sources=0 AND overseas_1_2>=0.90 AND local_depth_lift>=1.2
      AND source_systems=6 AND min_source_records>=0.8*records/6 AND max_source_records<=1.2*records/6,'PASS',
    raise_error(concat('records-per-person realism out of band: 1=',share_1,' 2-3=',share_2_3,' 4-8=',share_4_8,
      ' 9-20=',share_9_20,' 21-60=',share_21_60,' max=',max_records,' over3=',regulars_over_3_sources,
      ' overseas_1_2=',overseas_1_2,' lift=',local_depth_lift,' sources=',min_source_records,'-',max_source_records))) result
FROM stats s CROSS JOIN by_source b;

-- COMMAND ----------
-- Criterion 7: contact drift is low rate and chronologically ordered: for each guest and attribute,
-- every record showing the previous value was captured before every record showing the current one.
WITH flags AS (
  SELECT r.person_index,r.source_updated_at ts,a.attr,array_contains(r.drift_tags,a.attr) is_previous
  FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth r
  LATERAL VIEW explode(array('previous_personal_phone','previous_personal_email','previous_home_address','previous_employer')) a AS attr
), per_attr AS (
  SELECT person_index,attr,max(IF(is_previous,ts,NULL)) last_previous,min(IF(NOT is_previous,ts,NULL)) first_current
  FROM flags GROUP BY person_index,attr HAVING count_if(is_previous)>0
), stats AS (
  SELECT count_if(last_previous>=first_current) misordered,
    count(DISTINCT IF(first_current IS NOT NULL,person_index,NULL)) people_with_visible_drift,
    (SELECT count_if(record_count>=2) FROM {{catalog}}.{{truth_schema}}.guest_relationships) multi_record_people,
    count(DISTINCT IF(attr='previous_personal_phone' AND first_current IS NOT NULL,person_index,NULL)) phone_drift,
    count(DISTINCT IF(attr='previous_personal_email' AND first_current IS NOT NULL,person_index,NULL)) email_drift,
    count(DISTINCT IF(attr='previous_home_address' AND first_current IS NOT NULL,person_index,NULL)) address_drift,
    count(DISTINCT IF(attr='previous_employer' AND first_current IS NOT NULL,person_index,NULL)) employer_drift,
    -- No record is captured before the guest turned 18.
    (SELECT count(*) FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth r
      JOIN {{catalog}}.{{truth_schema}}.canonical_people c USING (person_index)
      WHERE r.source_updated_at<CAST(add_months(c.date_of_birth,216) AS TIMESTAMP)) underage_records,
    (SELECT count_if(source_updated_at>TIMESTAMP '2025-12-31 23:59:59' OR source_updated_at<TIMESTAMP '2019-01-01 00:00:00')
      FROM {{catalog}}.{{source_schema}}.source_identity_records) out_of_window_records
  FROM per_attr
)
SELECT 'contact_drift' gate,*,round(people_with_visible_drift/multi_record_people,4) drift_rate,
  IF(misordered=0 AND underage_records=0 AND out_of_window_records=0 AND people_with_visible_drift/multi_record_people BETWEEN 0.01 AND 0.15,'PASS',
    raise_error(concat('contact drift out of band: misordered=',misordered,' underage=',underage_records,' out_of_window=',out_of_window_records,' drifted=',people_with_visible_drift,
      ' multi_record_people=',multi_record_people))) result
FROM stats;
