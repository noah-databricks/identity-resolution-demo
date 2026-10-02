-- Databricks SQL generator. All random-looking choices are stable hashes.
CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{source_schema}}
COMMENT 'Blinded synthetic operational data; safe for the resolver';

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{truth_schema}}
COMMENT 'RESTRICTED: synthetic benchmark answer key; evaluator only';

-- COMMAND ----------
-- Weighted reference picks. Every reference row owns an integer range [lo, hi) of a
-- 10,000,000-wide wheel inside its group; a stable hash in [0, 10,000,000) selects it.
-- Rows are exploded into 100,000-wide buckets so each pick is an equi-join plus a range check.
CREATE OR REPLACE TEMP VIEW _pick_given AS
SELECT culture,sex,decade,name,lo,hi,explode(sequence(CAST(lo DIV 100000 AS INT),CAST((hi-1) DIV 100000 AS INT))) bucket
FROM {{catalog}}.{{reference_schema}}.given_names;

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW _pick_family AS
SELECT culture,name,lo,hi,explode(sequence(CAST(lo DIV 100000 AS INT),CAST((hi-1) DIV 100000 AS INT))) bucket
FROM {{catalog}}.{{reference_schema}}.family_names;

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW _pick_home_locality AS
SELECT w.culture,w.locality_key,l.suburb,l.state,l.postcode,l.sa4,l.apartment_pct,w.lo,w.hi,
  explode(sequence(CAST(w.lo DIV 100000 AS INT),CAST((w.hi-1) DIV 100000 AS INT))) bucket
FROM {{catalog}}.{{reference_schema}}.home_locality_weights w
JOIN {{catalog}}.{{reference_schema}}.au_localities l USING (locality_key);

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW _pick_email_domain AS
SELECT culture,age_band,domain,lo,hi,explode(sequence(CAST(lo DIV 100000 AS INT),CAST((hi-1) DIV 100000 AS INT))) bucket
FROM {{catalog}}.{{reference_schema}}.email_domains;

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW _pick_employer AS
SELECT *,explode(sequence(CAST(lo DIV 100000 AS INT),CAST((hi-1) DIV 100000 AS INT))) bucket
FROM {{catalog}}.{{reference_schema}}.employers WHERE lo>=0;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.household_membership
USING DELTA COMMENT 'RESTRICTED deterministic household archetypes and roles' AS
WITH people AS (
  SELECT CAST(id AS BIGINT) person_index,
    CAST(FLOOR(id/100) AS BIGINT) block_index,
    CAST(pmod(id,100) AS INT) block_position
  FROM range({{people}})
), blueprint AS (
  SELECT *,
    CASE
      WHEN block_position<15 THEN block_position
      WHEN block_position<43 THEN 15+CAST(FLOOR((block_position-15)/2) AS INT)
      WHEN block_position<79 THEN 29+CAST(FLOOR((block_position-43)/3) AS INT)
      WHEN block_position<91 THEN 41+CAST(FLOOR((block_position-79)/3) AS INT)
      WHEN block_position<96 THEN 45 ELSE 46 END local_household_index,
    CASE
      WHEN block_position<15 THEN 'solo'
      WHEN block_position<43 THEN 'couple'
      WHEN block_position<79 THEN 'family'
      WHEN block_position<91 THEN 'shared'
      ELSE 'multigenerational' END base_archetype,
    CASE
      WHEN block_position<15 THEN 0
      WHEN block_position<43 THEN pmod(block_position-15,2)
      WHEN block_position<79 THEN pmod(block_position-43,3)
      WHEN block_position<91 THEN pmod(block_position-79,3)
      WHEN block_position<96 THEN block_position-91 ELSE block_position-96 END base_member_ordinal
  FROM people
)
SELECT person_index,
  CASE
    WHEN person_index IN (30L,31L) THEN 900000030L
    -- Presenter guests with pinned birthdays live alone so household ages stay coherent.
    WHEN person_index IN (20L,22L,40L,50L,60L,70L) THEN 900000000L+person_index
    WHEN person_index BETWEEN 1400L AND 1799L THEN 910000000L+CAST(FLOOR((person_index-1400)/2) AS BIGINT)
    WHEN person_index BETWEEN 1800L AND 1999L THEN 920000000L+CAST(FLOOR((person_index-1800)/2) AS BIGINT)
    ELSE block_index*47L+local_household_index END household_index,
  CASE
    WHEN person_index IN (30L,31L) THEN 'family'
    WHEN person_index IN (20L,22L,40L,50L,60L,70L) THEN 'solo'
    WHEN person_index BETWEEN 1400L AND 1799L THEN 'shared'
    WHEN person_index BETWEEN 1800L AND 1999L THEN 'twins'
    ELSE base_archetype END household_archetype,
  CASE
    WHEN person_index IN (30L,31L) THEN CAST(person_index-30 AS INT)
    WHEN person_index IN (20L,22L,40L,50L,60L,70L) THEN 0
    WHEN person_index BETWEEN 1400L AND 1799L THEN CAST(pmod(person_index-1400,2) AS INT)
    WHEN person_index BETWEEN 1800L AND 1999L THEN CAST(pmod(person_index-1800,2) AS INT)
    ELSE base_member_ordinal END member_ordinal,
  CASE
    WHEN person_index=30L THEN 'parent'
    WHEN person_index=31L THEN 'adult_child'
    WHEN person_index IN (20L,22L,40L,50L,60L,70L) THEN 'resident'
    WHEN person_index BETWEEN 1400L AND 1799L THEN 'roommate'
    WHEN person_index BETWEEN 1800L AND 1999L THEN 'sibling'
    WHEN base_archetype='solo' THEN 'resident'
    WHEN base_archetype='couple' THEN 'partner'
    WHEN base_archetype='family' AND base_member_ordinal<2 THEN 'parent'
    WHEN base_archetype='family' THEN 'adult_child'
    WHEN base_archetype='shared' THEN 'roommate'
    WHEN base_archetype='multigenerational' AND base_member_ordinal=0 THEN 'older_relative'
    WHEN base_archetype='multigenerational' AND base_member_ordinal<3 THEN 'adult'
    ELSE 'younger_relative' END household_role
FROM blueprint;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.households
USING DELTA COMMENT 'RESTRICTED canonical synthetic households' AS
WITH grouped AS (
  SELECT household_index,max(household_archetype) household_archetype,count(*) household_size,
    -- Curated presenter households keep the addresses their pinned source records show.
    max(CASE WHEN person_index=10 THEN 'kr_seoul' WHEN person_index=70 THEN 'quay'
      WHEN person_index IN (30,31) THEN 'crown' WHEN person_index=50 THEN 'oxford'
      WHEN person_index=60 THEN 'bourke' WHEN person_index IN (20,22,40,3000) THEN 'anglo' END) hero_pin,
    -- Common-name challenge pairs (p, p+200) live in the same locality.
    min(CASE WHEN person_index BETWEEN 2800 AND 2999 THEN person_index-200 END) locality_anchor_person
  FROM {{catalog}}.{{truth_schema}}.household_membership
  GROUP BY household_index
), rolled AS (
  SELECT *,
    pmod(xxhash64('{{seed}}','surname_pattern',household_index),100) surname_roll,
    coalesce(hero_pin='kr_seoul',false) OR (hero_pin IS NULL AND pmod(xxhash64('{{seed}}','location_class',household_index),1000)<100) is_overseas,
    pmod(xxhash64('{{seed}}','overseas_pick',household_index),10000000) overseas_r,
    pmod(xxhash64('{{seed}}','culture_pick',household_index),10000000) culture_r,
    pmod(xxhash64('{{seed}}','home_locality',household_index),10000000) locality_r,
    pmod(xxhash64('{{seed}}','household_surname',household_index),10000000) surname_r,
    pmod(xxhash64('{{seed}}','street_pick',household_index),10000000) street_r,
    pmod(xxhash64('{{seed}}','street_type',household_index),10000000) street_type_r,
    pmod(xxhash64('{{seed}}','street_no_band',household_index),100) street_no_band,
    pmod(xxhash64('{{seed}}','street_no',household_index),1000000) street_no_r,
    pmod(xxhash64('{{seed}}','apartment',household_index),100) apartment_roll,
    pmod(xxhash64('{{seed}}','unit_no',household_index),1000) unit_r,
    pmod(xxhash64('{{seed}}','age_band',household_index),100) age_band_roll,
    pmod(xxhash64('{{seed}}','age_offset',household_index),1000) age_offset_r
  FROM grouped
), cultured AS (
  SELECT r.*,o.overseas_key,o.country overseas_country,o.city overseas_city,o.suburb overseas_suburb,
    o.region overseas_region,o.postcode overseas_postcode,o.phone_cc overseas_phone_cc,o.streets overseas_streets,
    CASE WHEN r.hero_pin='kr_seoul' THEN 'korean' WHEN r.hero_pin='quay' THEN 'chinese'
      WHEN r.hero_pin IS NOT NULL THEN 'anglo' WHEN r.is_overseas THEN o.culture ELSE c.culture END household_culture
  FROM rolled r
  LEFT JOIN {{catalog}}.{{reference_schema}}.overseas_localities o
    ON r.is_overseas AND ((r.hero_pin='kr_seoul' AND o.country='KR' AND o.suburb='Gangnam-gu')
      OR (r.hero_pin IS NULL AND r.overseas_r>=o.lo AND r.overseas_r<o.hi))
  LEFT JOIN {{catalog}}.{{reference_schema}}.cultures c
    ON NOT r.is_overseas AND r.culture_r>=c.lo AND r.culture_r<c.hi
), located AS (
  SELECT c.*,l.suburb au_suburb,l.state au_state,l.postcode au_postcode,l.sa4,
    coalesce(l.apartment_pct,40) apartment_pct,f.name household_family_name,
    s.street_name,s.has_type,t.street_type
  FROM cultured c
  LEFT JOIN _pick_home_locality l
    ON NOT c.is_overseas AND l.culture=c.household_culture AND l.bucket=CAST(c.locality_r DIV 100000 AS INT)
      AND c.locality_r>=l.lo AND c.locality_r<l.hi
  LEFT JOIN _pick_family f
    ON f.culture=c.household_culture AND f.bucket=CAST(c.surname_r DIV 100000 AS INT)
      AND c.surname_r>=f.lo AND c.surname_r<f.hi
  LEFT JOIN {{catalog}}.{{reference_schema}}.street_names s ON c.street_r>=s.lo AND c.street_r<s.hi
  LEFT JOIN {{catalog}}.{{reference_schema}}.street_types t ON c.street_type_r>=t.lo AND c.street_type_r<t.hi
), addressed AS (
  SELECT *,
    -- Street numbers are mostly small; a few long roads run into the hundreds.
    CASE WHEN street_no_band<62 THEN 1+pmod(street_no_r,48)
      WHEN street_no_band<90 THEN 49+pmod(street_no_r,152)
      ELSE 201+pmod(street_no_r,420) END street_no,
    IF(pmod(street_no_r,100)<3,element_at(array('A','B','C'),CAST(1+pmod(street_no_r DIV 100,3) AS INT)),'') street_no_suffix,
    apartment_roll<apartment_pct is_apartment,
    CASE WHEN unit_r<700 THEN 1+pmod(unit_r,14) WHEN unit_r<920 THEN 15+pmod(unit_r,36)
      ELSE (1+pmod(unit_r,24))*100+1+pmod(unit_r DIV 24,12) END unit_no,
    IF(has_type,street_name,concat(street_name,' ',street_type)) au_street,
    element_at(split(overseas_streets,'\\|'),CAST(1+pmod(street_r,size(split(overseas_streets,'\\|'))) AS INT)) overseas_street
  FROM located
), placed AS (
  SELECT *,
    CASE
      WHEN hero_pin='kr_seoul' THEN '152 Teheran-ro'
      WHEN hero_pin='quay' THEN '8 Quay Street'
      WHEN hero_pin='crown' THEN '14 Crown Street'
      WHEN hero_pin='oxford' THEN '7 Oxford Road'
      WHEN hero_pin='bourke' THEN '21 Bourke Street'
      WHEN NOT is_overseas THEN concat(IF(is_apartment,concat(unit_no,'/'),''),street_no,street_no_suffix,' ',au_street)
      WHEN overseas_country IN ('US') THEN concat(street_no,' ',overseas_street,IF(is_apartment,concat(' Apt ',unit_no),''))
      WHEN overseas_country IN ('GB','IE') THEN concat(IF(is_apartment,concat('Flat ',unit_no,', '),''),street_no,' ',overseas_street)
      WHEN overseas_country='CA' THEN concat(IF(is_apartment,concat(unit_no,'-'),''),street_no,' ',overseas_street)
      WHEN overseas_country='SG' THEN concat(street_no,' ',overseas_street,' #',lpad(CAST(2+pmod(unit_r,30) AS STRING),2,'0'),'-',lpad(CAST(1+pmod(unit_r DIV 30,20) AS STRING),2,'0'))
      WHEN overseas_country='HK' THEN concat('Flat ',element_at(array('A','B','C','D'),CAST(1+pmod(unit_r,4) AS INT)),', ',3+pmod(unit_r,38),'/F, ',street_no,' ',overseas_street)
      WHEN overseas_country='CN' THEN concat('No. ',street_no,' ',overseas_street)
      WHEN overseas_country='JP' THEN concat(1+pmod(street_no_r,6),'-',1+pmod(street_no_r DIV 6,20),'-',1+pmod(unit_r,15),' ',overseas_street)
      WHEN overseas_country IN ('DE','NL','IT','ES','GR') THEN concat(overseas_street,' ',street_no)
      WHEN overseas_country='ID' THEN concat(overseas_street,' No. ',street_no)
      WHEN overseas_country='AE' THEN concat('Apt ',unit_no,', ',overseas_street)
      ELSE concat(street_no,' ',overseas_street) END address_line1,
    CASE WHEN hero_pin='kr_seoul' THEN 'Gangnam-gu' WHEN hero_pin IN ('quay','oxford') THEN 'Sydney'
      WHEN hero_pin='crown' THEN 'Surry Hills' WHEN hero_pin='bourke' THEN 'Alexandria'
      WHEN is_overseas THEN overseas_suburb ELSE au_suburb END suburb,
    CASE WHEN hero_pin='kr_seoul' THEN 'Seoul' WHEN hero_pin IN ('quay','oxford','crown','bourke') THEN 'NSW'
      WHEN is_overseas THEN overseas_region ELSE au_state END state,
    CASE WHEN hero_pin='kr_seoul' THEN '06236' WHEN hero_pin IN ('quay','oxford') THEN '2000'
      WHEN hero_pin='crown' THEN '2010' WHEN hero_pin='bourke' THEN '2015'
      WHEN is_overseas THEN nullif(overseas_postcode,'') ELSE au_postcode END postcode,
    CASE WHEN is_overseas THEN overseas_country ELSE 'AU' END country,
    CASE WHEN is_overseas THEN 'overseas' WHEN sa4 LIKE 'Sydney - %' OR sa4='Central Coast' OR hero_pin IN ('quay','crown','oxford','bourke') THEN 'greater_sydney'
      ELSE 'other_au' END location_class,
    -- Household anchor age (years at 2025-12-31); members derive their ages from it.
    CASE household_archetype
      WHEN 'solo' THEN CASE WHEN age_band_roll<55 THEN 22+pmod(age_offset_r,19) WHEN age_band_roll<85 THEN 41+pmod(age_offset_r,25) ELSE 66+pmod(age_offset_r,20) END
      WHEN 'couple' THEN CASE WHEN age_band_roll<50 THEN 25+pmod(age_offset_r,16) WHEN age_band_roll<80 THEN 41+pmod(age_offset_r,20) ELSE 61+pmod(age_offset_r,22) END
      WHEN 'family' THEN 42+pmod(age_offset_r,23)
      WHEN 'multigenerational' THEN 70+pmod(age_offset_r,16)
      ELSE 20+pmod(age_offset_r,15) END anchor_age
  FROM addressed
), anchored AS (
  -- The second member of each common-name pair adopts the first member's locality.
  SELECT p.*,a.household_index anchor_household_index
  FROM placed p
  LEFT JOIN {{catalog}}.{{truth_schema}}.household_membership am ON am.person_index=p.locality_anchor_person
  LEFT JOIN placed a ON a.household_index=am.household_index
)
SELECT p.household_index,
  sha2(concat('{{seed}}', ':household:', p.household_index), 256) truth_household_id,
  p.household_archetype,p.household_size,
  CASE
    WHEN p.household_size=1 THEN 'individual'
    WHEN p.household_archetype='couple' AND p.surname_roll<35 THEN 'same'
    WHEN p.household_archetype='family' AND p.surname_roll<65 THEN 'same'
    WHEN p.household_archetype='family' AND p.surname_roll<95 THEN 'mixed'
    WHEN p.household_archetype='multigenerational' AND p.surname_roll<35 THEN 'same'
    WHEN p.household_archetype='multigenerational' AND p.surname_roll<85 THEN 'mixed'
    WHEN p.household_archetype='twins' THEN 'same'
    ELSE 'independent' END surname_pattern,
  p.household_culture,p.household_family_name,p.anchor_age,p.is_apartment,
  coalesce(a.address_line1,p.address_line1) address_line1,
  coalesce(a.suburb,p.suburb) suburb,
  IF(a.household_index IS NOT NULL,a.state,p.state) state,
  IF(a.household_index IS NOT NULL,a.postcode,p.postcode) postcode,
  coalesce(a.country,p.country) country,
  coalesce(a.location_class,p.location_class) location_class,
  IF(coalesce(a.country,p.country)='AU',NULL,coalesce(a.overseas_phone_cc,p.overseas_phone_cc)) overseas_phone_cc,
  concat('+61 2 9',lpad(CAST(pmod(xxhash64('{{seed}}','home_phone',p.household_index),10000000) AS STRING),7,'0')) household_phone
FROM anchored p
LEFT JOIN placed a ON a.household_index=p.anchor_household_index;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.canonical_people
USING DELTA COMMENT 'RESTRICTED coherent canonical synthetic people' AS
WITH members AS (
  SELECT m.person_index,m.household_index,m.household_archetype,m.member_ordinal,m.household_role,
    h.truth_household_id,h.household_size,h.surname_pattern,h.household_culture,h.household_family_name,
    h.anchor_age,h.address_line1,h.suburb,h.state,h.postcode,h.country,h.location_class,h.overseas_phone_cc,
    IF(m.person_index BETWEEN 2600L AND 2999L,CAST(pmod(m.person_index-2600,200) AS INT),NULL) common_name_pair,
    IF(m.person_index BETWEEN 1800L AND 1999L,CAST(FLOOR((m.person_index-1800)/2) AS INT),NULL) twin_pair,
    pmod(xxhash64('{{seed}}','household_sex',m.household_index),2) household_sex_bit
  FROM {{catalog}}.{{truth_schema}}.household_membership m
  JOIN {{catalog}}.{{truth_schema}}.households h USING (household_index)
), sexed AS (
  SELECT mb.*,tw.first_name twin_first,tw.second_name twin_second,tw.sex_rule twin_sex_rule,
    CASE
      WHEN person_index IN (10,22,30,3000) THEN 'M'
      WHEN person_index IN (20,31,40,50,60,70) THEN 'F'
      WHEN twin_pair IS NOT NULL THEN CASE WHEN tw.sex_rule IN ('M','F') THEN tw.sex_rule WHEN member_ordinal=0 THEN 'M' ELSE 'F' END
      WHEN common_name_pair IS NOT NULL THEN IF(pmod(xxhash64('{{seed}}','common_name_sex',common_name_pair),2)=0,'F','M')
      WHEN household_archetype IN ('couple','family') AND member_ordinal<2 THEN
        IF((household_sex_bit=0)=(member_ordinal=0 OR pmod(xxhash64('{{seed}}','same_sex_partner',household_index),100)<7),'F','M')
      ELSE IF(pmod(xxhash64('{{seed}}','sex',person_index),2)=0,'F','M') END sex,
    CASE
      WHEN household_archetype IN ('solo','twins') OR household_role='resident' THEN anchor_age
      WHEN household_archetype='couple' THEN IF(member_ordinal=0,anchor_age,anchor_age-4+pmod(xxhash64('{{seed}}','partner_age',person_index),11))
      WHEN household_role='parent' THEN IF(member_ordinal=0,anchor_age,anchor_age-4+pmod(xxhash64('{{seed}}','parent_age',person_index),9))
      WHEN household_role='adult_child' THEN least(anchor_age-22,greatest(18,anchor_age-22-pmod(xxhash64('{{seed}}','child_age',person_index),17)))
      WHEN household_role='older_relative' THEN anchor_age
      WHEN household_role='adult' THEN anchor_age-22-pmod(xxhash64('{{seed}}','adult_age',person_index),12)
      WHEN household_role='younger_relative' THEN greatest(18,least(anchor_age-52,18+pmod(xxhash64('{{seed}}','younger_age',person_index),9)))
      ELSE 20+pmod(xxhash64('{{seed}}','roommate_age',person_index),15) END age_years
  FROM members mb
  LEFT JOIN {{catalog}}.{{reference_schema}}.twin_names tw
    ON mb.twin_pair IS NOT NULL AND tw.pair_key=pmod(xxhash64('{{seed}}','twin_names',mb.twin_pair),25)
), dated AS (
  SELECT *,
    CASE
      WHEN person_index=10 THEN DATE '1991-02-14'
      WHEN person_index=20 THEN DATE '1988-03-11'
      WHEN person_index=22 THEN DATE '1990-07-22'
      WHEN person_index=30 THEN DATE '1974-09-09'
      WHEN person_index=31 THEN DATE '2002-12-12'
      WHEN person_index=40 THEN DATE '1989-11-23'
      WHEN person_index=50 THEN DATE '1994-06-18'
      WHEN person_index=60 THEN DATE '1986-08-04'
      WHEN person_index=70 THEN DATE '1992-02-29'
      WHEN person_index=3000 THEN DATE '1978-03-22'
      WHEN person_index BETWEEN 1800L AND 1999L THEN date_add(DATE '1985-01-01',CAST(pmod(CAST(FLOOR((person_index-1800)/2) AS BIGINT)*397,5000) AS INT))
      ELSE date_sub(add_months(DATE '2025-12-31',-12*CAST(greatest(18,least(85,age_years)) AS INT)),
        CAST(pmod(xxhash64('{{seed}}','birthday',person_index),365) AS INT))
    END date_of_birth
  FROM sexed
), cultured AS (
  SELECT d.*,
    -- Partners and roommates are not always from the household's heritage.
    CASE
      WHEN person_index IN (10) THEN 'korean' WHEN person_index IN (70) THEN 'chinese'
      WHEN person_index IN (20,22,30,31,40,50,60,3000) OR common_name_pair IS NOT NULL THEN 'anglo'
      WHEN household_role='roommate' OR (household_archetype IN ('couple','family') AND member_ordinal=1
        AND pmod(xxhash64('{{seed}}','mixed_partner',person_index),100)<14) THEN c.culture
      ELSE household_culture END person_culture
  FROM dated d
  LEFT JOIN {{catalog}}.{{reference_schema}}.cultures c
    ON pmod(xxhash64('{{seed}}','person_culture',d.person_index),10000000)>=c.lo
      AND pmod(xxhash64('{{seed}}','person_culture',d.person_index),10000000)<c.hi
), given_ready AS (
  SELECT x.*,
    CASE WHEN x.person_culture='anglo' OR pmod(xxhash64('{{seed}}','anglicised_given',x.person_index),100)<
        IF(year(x.date_of_birth)<1975,c.anglicised_pct_pre1975,c.anglicised_pct_post1975) THEN 'anglo'
      ELSE x.person_culture END given_culture,
    CAST(least(2000,greatest(1940,FLOOR(year(x.date_of_birth)/10)*10)) AS INT) birth_decade,
    CASE WHEN x.common_name_pair IS NOT NULL THEN pmod(xxhash64('{{seed}}','common_given',x.common_name_pair),30)
      ELSE NULL END common_given_rank,
    CASE WHEN x.common_name_pair IS NOT NULL THEN pmod(xxhash64('{{seed}}','common_family',x.common_name_pair),40)
      ELSE NULL END common_family_rank,
    pmod(xxhash64('{{seed}}','given_pick',x.person_index),10000000) given_r,
    pmod(xxhash64('{{seed}}','family_pick',x.person_index),10000000) family_r
  FROM cultured x
  LEFT JOIN {{catalog}}.{{reference_schema}}.cultures c ON c.culture=x.person_culture
), common_given AS (
  SELECT sex,name,CAST(row_number() OVER (PARTITION BY sex ORDER BY hi-lo DESC,name)-1 AS BIGINT) rk
  FROM {{catalog}}.{{reference_schema}}.given_names WHERE culture='anglo' AND decade=1980
  QUALIFY row_number() OVER (PARTITION BY sex ORDER BY hi-lo DESC,name)<=30
), common_family AS (
  SELECT name,CAST(row_number() OVER (ORDER BY hi-lo DESC,name)-1 AS BIGINT) rk
  FROM {{catalog}}.{{reference_schema}}.family_names WHERE culture='anglo'
  QUALIFY row_number() OVER (ORDER BY hi-lo DESC,name)<=40
), named AS (
  SELECT g.*,
    CASE person_index
      WHEN 10 THEN 'Minjun'
      WHEN 20 THEN 'Alice'
      WHEN 22 THEN 'Marcus'
      WHEN 30 THEN 'James'
      WHEN 31 THEN 'Jasmine'
      WHEN 40 THEN 'Sophie'
      WHEN 50 THEN 'Matilda'
      WHEN 60 THEN 'Olivia'
      WHEN 70 THEN 'Yutong'
      WHEN 3000 THEN 'Sam'
      ELSE CASE
        WHEN g.twin_pair IS NOT NULL THEN IF(g.member_ordinal=0,g.twin_first,g.twin_second)
        WHEN g.common_name_pair IS NOT NULL THEN cg.name
        ELSE gn.name
      END END given_name,
    CASE person_index
      WHEN 10 THEN 'Kim'
      WHEN 20 THEN 'Howard'
      WHEN 22 THEN 'Reed'
      WHEN 30 THEN 'Walker'
      WHEN 31 THEN 'Walker'
      WHEN 40 THEN 'Martin'
      WHEN 50 THEN 'Wilson'
      WHEN 60 THEN 'Brown'
      WHEN 70 THEN 'Zhang'
      WHEN 3000 THEN 'Taylor'
      ELSE CASE
        WHEN g.common_name_pair IS NOT NULL THEN cf.name
        WHEN g.surname_pattern='same' OR (g.surname_pattern='mixed' AND g.member_ordinal<2) OR g.surname_pattern='individual' THEN g.household_family_name
        ELSE fn.name
      END
    END family_name
  FROM given_ready g
  LEFT JOIN _pick_given gn
    ON gn.culture=g.given_culture AND gn.sex=g.sex AND gn.decade=IF(g.given_culture='anglo',g.birth_decade,0)
      AND gn.bucket=CAST(g.given_r DIV 100000 AS INT) AND g.given_r>=gn.lo AND g.given_r<gn.hi
  LEFT JOIN _pick_family fn
    ON fn.culture=g.person_culture AND fn.bucket=CAST(g.family_r DIV 100000 AS INT)
      AND g.family_r>=fn.lo AND g.family_r<fn.hi
  LEFT JOIN common_given cg ON cg.sex=g.sex AND cg.rk=g.common_given_rank
  LEFT JOIN common_family cf ON cf.rk=g.common_family_rank
), employed AS (
  SELECT n.*,
    floor(months_between(DATE '2025-12-31',n.date_of_birth)/12) age,
    CASE
      WHEN person_index IN (20,22) THEN 0L
      WHEN person_index=3000 THEN 1L
      WHEN person_index BETWEEN 1000 AND 1399 THEN -10L
      WHEN person_index BETWEEN 2000 AND 2199 THEN -20L
      WHEN person_index BETWEEN 3001 AND 3299 THEN -30L
      WHEN n.country<>'AU' THEN NULL
      WHEN pmod(xxhash64('{{seed}}','employed',person_index),100)<
        CASE WHEN floor(months_between(DATE '2025-12-31',n.date_of_birth)/12)<23 THEN 35
          WHEN floor(months_between(DATE '2025-12-31',n.date_of_birth)/12)<65 THEN 74
          WHEN floor(months_between(DATE '2025-12-31',n.date_of_birth)/12)<71 THEN 25 ELSE 4 END THEN -30L
      ELSE NULL END employer_directive
  FROM named n
), with_employer AS (
  SELECT e.*,coalesce(fixed.employer_key,pk.employer_key) employer_key
  FROM employed e
  LEFT JOIN {{catalog}}.{{reference_schema}}.employers fixed ON e.employer_directive>=0 AND fixed.employer_key=e.employer_directive
  LEFT JOIN _pick_employer pk
    ON e.employer_directive<0
      AND pk.bucket=CAST(pmod(xxhash64('{{seed}}','employer_pick',CASE e.employer_directive
            WHEN -10L THEN CAST(FLOOR((e.person_index-1000)/10) AS BIGINT)+900000000L
            WHEN -20L THEN CAST(FLOOR((e.person_index-2000)/2) AS BIGINT)+800000000L
            ELSE e.person_index END),10000000) DIV 100000 AS INT)
      AND pmod(xxhash64('{{seed}}','employer_pick',CASE e.employer_directive
            WHEN -10L THEN CAST(FLOOR((e.person_index-1000)/10) AS BIGINT)+900000000L
            WHEN -20L THEN CAST(FLOOR((e.person_index-2000)/2) AS BIGINT)+800000000L
            ELSE e.person_index END),10000000) BETWEEN pk.lo AND pk.hi-1
), emailed AS (
  SELECT w.*,
    -- Presenter guests keep the personal addresses their pinned source records show.
    coalesce(element_at(map(10L,'gmail.com',20L,'gmail.com',22L,'outlook.com',30L,'gmail.com',31L,'hotmail.com',
      40L,'icloud.com',50L,'gmail.com',60L,'gmail.com',70L,'hotmail.com',3000L,'gmail.com'),w.person_index),d.domain) personal_email_domain,
    CASE
      WHEN w.person_index IN (10,20,22,30,31,40,50,60,70,3000) THEN 'NAME_BASED'
      WHEN pmod(xxhash64('{{seed}}','email_origin',w.person_index),100)<94 THEN 'NAME_BASED'
      WHEN pmod(xxhash64('{{seed}}','email_origin',w.person_index),100)<98 THEN 'INITIALS'
      ELSE 'OPAQUE_LEGACY' END personal_email_origin,
    regexp_replace(lower(w.given_name),'[^a-z]','') gtok,
    regexp_replace(lower(w.family_name),'[^a-z]','') ftok,
    pmod(xxhash64('{{seed}}','email_style',w.person_index),1000) email_style,
    pmod(xxhash64('{{seed}}','email_number',w.person_index),100) email_number_kind,
    pmod(xxhash64('{{seed}}','email_small_a',w.person_index),60) small_a,
    pmod(xxhash64('{{seed}}','email_small_b',w.person_index),60) small_b
  FROM with_employer w
  LEFT JOIN _pick_email_domain d
    ON d.culture=w.person_culture
      AND d.age_band=CASE WHEN w.age<35 THEN 'under_35' WHEN w.age<55 THEN '35_to_54' ELSE '55_plus' END
      AND d.bucket=CAST(pmod(xxhash64('{{seed}}','email_domain',w.person_index),10000000) DIV 100000 AS INT)
      AND pmod(xxhash64('{{seed}}','email_domain',w.person_index),10000000) BETWEEN d.lo AND d.hi-1
), numbered AS (
  SELECT *,
    -- Plausible suffixes: birth year (2 or 4 digits), small integers, a few favourite numbers.
    CASE WHEN email_number_kind<42 THEN substr(CAST(year(date_of_birth) AS STRING),3,2)
      WHEN email_number_kind<58 THEN CAST(year(date_of_birth) AS STRING)
      WHEN email_number_kind<88 THEN CAST(1+least(small_a,small_b) AS STRING)
      ELSE CAST(element_at(array(7,11,12,13,21,22,23,24,27,33,77,88,99),CAST(1+pmod(small_a,13) AS INT)) AS STRING) END email_number,
    CASE WHEN email_number_kind<50 THEN CAST(1+least(small_a,small_b) AS STRING)
      ELSE substr(CAST(year(date_of_birth) AS STRING),3,2) END alt_number
  FROM emailed
), candidates AS (
  SELECT *,
    CASE WHEN person_index IN (10,20,22,30,31,40,50,60,70,3000) THEN element_at(map(10L,'minjun.kim',20L,'alice.howard',
        22L,'marcus.reed',30L,'james.walker',31L,'jasmine.walker',40L,'sophie.martin',50L,'matilda.wilson',60L,'olivia.brown',
        70L,'yutong.zhang',3000L,'sam.taylor2'),person_index)
      ELSE CASE personal_email_origin
      WHEN 'INITIALS' THEN concat(substr(gtok,1,1),substr(ftok,1,1),email_number)
      WHEN 'OPAQUE_LEGACY' THEN concat(fallback_prefix,email_number)
      ELSE CASE
        WHEN email_style<340 THEN concat(gtok,'.',ftok)
        WHEN email_style<470 THEN concat(gtok,ftok)
        WHEN email_style<505 THEN concat(gtok,'_',ftok)
        WHEN email_style<600 THEN concat(substr(gtok,1,1),ftok)
        WHEN email_style<640 THEN concat(gtok,substr(ftok,1,1))
        WHEN email_style<670 THEN concat(ftok,'.',gtok)
        WHEN email_style<690 THEN concat(ftok,substr(gtok,1,1))
        WHEN email_style<715 THEN concat(substr(gtok,1,1),'.',ftok)
        WHEN email_style<740 THEN concat(gtok,'.',substr(ftok,1,1))
        WHEN email_style<765 THEN gtok || substr(ftok,1,3)
        WHEN email_style<830 THEN concat(gtok,'.',ftok,email_number)
        WHEN email_style<890 THEN concat(gtok,ftok,email_number)
        WHEN email_style<950 THEN concat(gtok,email_number)
        ELSE concat(substr(gtok,1,1),ftok,email_number) END END END local1,
    CASE WHEN personal_email_origin<>'NAME_BASED' THEN concat(fallback_prefix,alt_number)
      WHEN email_style<830 THEN
        concat(CASE WHEN email_style<340 THEN concat(gtok,'.',ftok) WHEN email_style<600 THEN concat(gtok,ftok) ELSE concat(substr(gtok,1,1),ftok) END,alt_number)
      ELSE concat(gtok,ftok,alt_number) END local2
  FROM (SELECT *,
      CASE personal_email_origin
        WHEN 'INITIALS' THEN concat(substr(gtok,1,1),substr(ftok,1,1))
        WHEN 'OPAQUE_LEGACY' THEN concat(
          element_at(array('surfer','bondi','coffee','sunny','blue','harbour','footy','netball','wanderer','tigers','swans','waratah','coastal','sparkle','happy','lucky','ocean','summer','koala','salty','island','kombi','vintage','crazy'),CAST(1+pmod(small_a,24) AS INT)),
          element_at(array('girl','boy','lover','fan','life','dreams','vibes','chick','dude','mum','dad','queen','king','kid','soul'),CAST(1+pmod(small_b,15) AS INT)))
        ELSE concat(gtok,'.',ftok) END fallback_prefix
    FROM numbered) fp
), resolved1 AS (
  SELECT *,concat(local1,'@',personal_email_domain) email1,
    row_number() OVER (PARTITION BY concat(local1,'@',personal_email_domain) ORDER BY person_index) rn1
  FROM candidates
), resolved2 AS (
  SELECT *,IF(rn1=1,email1,concat(local2,'@',personal_email_domain)) email2
  FROM resolved1
), ranked2 AS (
  SELECT *,row_number() OVER (PARTITION BY email2 ORDER BY IF(rn1=1,0,1),person_index) rn2 FROM resolved2
), resolved3 AS (
  SELECT *,IF(rn2=1,email2,concat(fallback_prefix,CAST(100+pmod(xxhash64('{{seed}}','email_tail',person_index),900) AS STRING),'@',personal_email_domain)) email3
  FROM ranked2
), ranked3 AS (
  SELECT *,row_number() OVER (PARTITION BY email3 ORDER BY IF(rn2=1,0,1),person_index) rn3 FROM resolved3
), work_ready AS (
  SELECT r.*,IF(rn3=1,email3,concat(replace(fallback_prefix,'.',''),CAST(person_index AS STRING),'@',personal_email_domain)) personal_email_final,
    rn1>1 personal_email_collision_bumped,
    em.company_name,em.domain work_domain,em.email_pattern,em.office_line1,em.office_suburb,em.office_state,
    em.office_postcode,em.area_code,
    CASE em.email_pattern
      WHEN 'first.last' THEN concat(gtok,'.',ftok)
      WHEN 'flast' THEN concat(substr(gtok,1,1),ftok)
      WHEN 'firstl' THEN concat(gtok,substr(ftok,1,1))
      WHEN 'first' THEN gtok
      WHEN 'first_last' THEN concat(gtok,'_',ftok)
      WHEN 'firstlast' THEN concat(gtok,ftok)
      WHEN 'last.first' THEN concat(ftok,'.',gtok) END work_local1
  FROM ranked3 r
  LEFT JOIN {{catalog}}.{{reference_schema}}.employers em ON em.employer_key=r.employer_key
), work_ranked AS (
  SELECT *,row_number() OVER (PARTITION BY work_domain,work_local1 ORDER BY person_index) work_rn1 FROM work_ready
), work_resolved AS (
  SELECT *,
    CASE WHEN work_domain IS NULL THEN NULL
      WHEN work_rn1=1 THEN concat(work_local1,'@',work_domain)
      ELSE concat(CASE WHEN email_pattern IN ('first','firstl') THEN concat(gtok,'.',ftok)
          ELSE concat(gtok,'.',element_at(split('abcdefghijklmnoprstw',''),CAST(1+pmod(xxhash64('{{seed}}','middle_initial',person_index),20) AS INT)),'.',ftok) END,
        '@',work_domain) END work_email2
  FROM work_ranked
), work_final AS (
  SELECT *,row_number() OVER (PARTITION BY work_email2 ORDER BY IF(work_rn1=1,0,1),person_index) work_rn2 FROM work_resolved
)
SELECT truth_person_id,person_index,given_name,family_name,sex,person_culture name_culture,given_culture,
  date_of_birth,truth_household_id,household_index,household_archetype,household_role,member_ordinal,
  location_class,personal_email_origin,personal_email_collision_bumped,
  personal_email,
  CASE WHEN work_email2 IS NULL THEN NULL WHEN work_rn2=1 THEN work_email2
    ELSE concat(split(work_email2,'@')[0],CAST(work_rn2 AS STRING),'@',work_domain) END work_email,
  CASE WHEN country='AU' THEN concat('+61 4',substr(mobile_digits,1,2),' ',substr(mobile_digits,3,3),' ',substr(mobile_digits,6,3))
    ELSE concat(overseas_phone_cc,' ',CASE overseas_phone_cc
      WHEN '+64' THEN concat('21 ',substr(od,1,3),' ',substr(od,4,4))
      WHEN '+44' THEN concat('7',substr(od,1,3),' ',substr(od,4,6))
      WHEN '+1' THEN concat(element_at(array('212','415','310','646','604','416'),CAST(1+pmod(person_index,6) AS INT)),' ',substr(od,1,3),' ',substr(od,4,4))
      WHEN '+65' THEN concat('9',substr(od,1,3),' ',substr(od,4,4))
      WHEN '+852' THEN concat('9',substr(od,1,3),' ',substr(od,4,4))
      WHEN '+86' THEN concat('13',substr(od,1,1),' ',substr(od,2,4),' ',substr(od,6,4))
      WHEN '+82' THEN concat('10-',substr(od,1,4),'-',substr(od,5,4))
      WHEN '+81' THEN concat('90-',substr(od,1,4),'-',substr(od,5,4))
      ELSE concat('9',substr(od,1,4),' ',substr(od,5,4)) END) END personal_phone,
  IF(company_name IS NULL,NULL,concat('+61 ',area_code,' ',
    -- Direct lines follow each state's numbering (Sydney/Melbourne 8 or 9, Canberra 6, Brisbane 3, Perth 9, Adelaide 8).
    CASE office_state WHEN 'ACT' THEN '6' WHEN 'QLD' THEN '3' WHEN 'WA' THEN '9' WHEN 'SA' THEN '8'
      ELSE IF(pmod(xxhash64('{{seed}}','did_block',employer_key),2)=0,'9','8') END,
    lpad(CAST(pmod(xxhash64('{{seed}}','did_block',employer_key),1000) AS STRING),3,'0'),' ',
    lpad(CAST(pmod(xxhash64('{{seed}}','did',person_index),10000) AS STRING),4,'0'))) work_phone,
  address_line1 home_address_line1,suburb home_suburb,state home_state,postcode home_postcode,country home_country,
  office_line1 work_address_line1,office_suburb work_suburb,office_state work_state,office_postcode work_postcode,
  company_name
FROM (
  SELECT *,sha2(concat('{{seed}}',':person:',person_index),256) truth_person_id,
    personal_email_final personal_email,
    -- AU mobile: real 04xx prefix plus a person-unique six-digit subscriber number.
    concat(element_at(array('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19',
        '20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39',
        '47','48','49','50','51','52','53','55','57','58','59','66','67','68','69','70','71','72','73','74','75',
        '76','77','78','79','80','81','82','83','84','85','87','88','89','90','91','92','93'),
        CAST(1+pmod(xxhash64('{{seed}}','mobile_prefix',person_index),78) AS INT)),
      lpad(CAST(pmod(person_index*386093+104729,1000000) AS STRING),6,'0')) mobile_digits,
    concat(lpad(CAST(pmod(person_index*386093+104729,1000000) AS STRING),6,'0'),
      lpad(CAST(pmod(xxhash64('{{seed}}','overseas_mobile',person_index),1000) AS STRING),3,'0')) od
  FROM work_final
) x;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.employment_history
USING DELTA COMMENT 'RESTRICTED canonical employment history including changes' AS
WITH current_jobs AS (
  SELECT p.*,e.employer_key,
    pmod(xxhash64('{{seed}}','job_change',p.person_index),100)<18 AND p.person_index NOT IN (20,22,3000) moved,
    -- Scenario cohorts keep the v4 mid-2023 move; the general population changes jobs any time from mid-2019.
    IF(p.person_index<3300,DATE '2023-07-01',
      least(DATE '2025-12-31',greatest(add_months(p.date_of_birth,228),
        date_add(DATE '2019-07-01',CAST(pmod(xxhash64('{{seed}}','job_change_date',p.person_index),2190) AS INT))))) move_date,
    -- Nobody holds a job before turning 18.
    greatest(DATE '2018-01-01',add_months(p.date_of_birth,216)) career_start,
    regexp_replace(lower(p.given_name),'[^a-z]','') gtok,regexp_replace(lower(p.family_name),'[^a-z]','') ftok
  FROM {{catalog}}.{{truth_schema}}.canonical_people p
  JOIN {{catalog}}.{{reference_schema}}.employers e ON e.company_name=p.company_name
), previous_jobs AS (
  SELECT c.*,e.company_name previous_company,e.domain previous_domain,e.office_line1 previous_line1,
    e.office_suburb previous_suburb,e.office_state previous_state,e.office_postcode previous_postcode,e.area_code previous_area,
    -- The previous mailbox follows the previous employer's own address convention.
    CASE e.email_pattern
      WHEN 'first.last' THEN concat(gtok,'.',ftok) WHEN 'flast' THEN concat(substr(gtok,1,1),ftok)
      WHEN 'firstl' THEN concat(gtok,substr(ftok,1,1)) WHEN 'first' THEN gtok
      WHEN 'first_last' THEN concat(gtok,'_',ftok) WHEN 'firstlast' THEN concat(gtok,ftok)
      WHEN 'last.first' THEN concat(ftok,'.',gtok) ELSE concat(gtok,'.',ftok) END previous_local
  FROM current_jobs c
  JOIN {{catalog}}.{{reference_schema}}.employers e
    ON e.employer_key=2+pmod(xxhash64('{{seed}}','previous_employer',c.person_index),340)
  WHERE c.moved AND e.employer_key<>c.employer_key
)
SELECT c.truth_person_id,IF(pj.truth_person_id IS NOT NULL,2,1) employment_sequence,c.company_name,c.work_email,c.work_phone,
  c.work_address_line1,c.work_suburb,c.work_state,c.work_postcode,
  IF(pj.truth_person_id IS NOT NULL,c.move_date,c.career_start) valid_from,CAST(NULL AS DATE) valid_to
FROM current_jobs c
LEFT JOIN previous_jobs pj ON pj.truth_person_id=c.truth_person_id
UNION ALL
SELECT truth_person_id,1,previous_company,concat(previous_local,'@',previous_domain),
  concat('+61 ',previous_area,' 9',lpad(CAST(pmod(xxhash64('{{seed}}','previous_did',person_index),10000000) AS STRING),7,'0')),
  previous_line1,previous_suburb,previous_state,previous_postcode,least(career_start,date_sub(move_date,180)),date_sub(move_date,1)
FROM previous_jobs;

-- COMMAND ----------
-- Relationship depth (generator v4.1). Records per guest are heavy tailed: most guests
-- appear once, regulars accumulate profiles across a few systems over several years.
-- Venue-area residents and hospitality workers visit more; overseas visitors rarely return.
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.guest_relationships
USING DELTA COMMENT 'RESTRICTED per-guest relationship depth, source affinity, record timeline and contact drift' AS
WITH base AS (
  SELECT p.person_index,p.truth_person_id,p.home_country,p.company_name,p.name_culture,p.personal_email,
    floor(months_between(DATE '2025-12-31',p.date_of_birth)/12) age,
    -- Guests only appear from their 18th birthday; the window is 2019-01-01 (day 0) to 2025-12-31 (day 2555).
    CAST(greatest(0,datediff(add_months(p.date_of_birth,216),DATE '2019-01-01')) AS INT) adult_from_day,
    p.person_index IN (10,20,22,30,31,40,50,60,70,3000) pinned_hero,
    -- Scenario cohorts (person_index < 3300) keep their v4 slots 0-2, sources and timestamps.
    p.person_index<3300 scenario_cohort,
    CASE WHEN p.location_class='overseas' THEN 'overseas_visitor'
      WHEN p.location_class='greater_sydney' AND p.company_name IS NULL
        AND floor(months_between(DATE '2025-12-31',p.date_of_birth)/12) BETWEEN 18 AND 40
        AND pmod(xxhash64('{{seed}}','hospitality_worker',p.person_index),100)<20 THEN 'hospitality_worker'
      WHEN a.sa4 IN ('Sydney - Eastern Suburbs','Sydney - City and Inner South','Sydney - Inner West','Sydney - Northern Beaches')
        THEN 'venue_local'
      WHEN p.location_class='greater_sydney' THEN 'sydney_resident'
      ELSE 'interstate_or_regional' END visit_segment,
    pmod(xxhash64('{{seed}}','visit_tier',p.person_index),10000) tier_roll,
    pmod(xxhash64('{{seed}}','visit_depth',p.person_index),10000)/10000.0 depth_u
  FROM {{catalog}}.{{truth_schema}}.canonical_people p
  LEFT JOIN {{catalog}}.{{reference_schema}}.au_localities a
    ON p.home_country='AU' AND a.suburb=p.home_suburb AND a.state=p.home_state AND a.postcode=p.home_postcode
), drawn AS (
  SELECT *,
    -- Cumulative per-10,000 cut points for 1 | 2-3 | 4-8 | 9-20 records; the remainder has 21-60.
    CASE visit_segment
      WHEN 'overseas_visitor' THEN array(8800,9880,10000,10000)
      WHEN 'interstate_or_regional' THEN array(7300,9350,9880,9985)
      WHEN 'sydney_resident' THEN array(6600,8800,9690,9960)
      WHEN 'venue_local' THEN array(5800,8000,9250,9870)
      ELSE array(3500,6000,8000,9400) END cuts
  FROM base
), counted AS (
  SELECT *,
    CASE WHEN pinned_hero THEN 3 WHEN scenario_cohort THEN greatest(3,drawn_count) ELSE drawn_count END record_count
  FROM (SELECT *,
      CASE WHEN tier_roll<cuts[0] THEN 1
        WHEN tier_roll<cuts[1] THEN IF(pmod(xxhash64('{{seed}}','visit_pair',person_index),100)<IF(visit_segment='overseas_visitor',90,66),2,3)
        WHEN tier_roll<cuts[2] THEN 4+CAST(FLOOR(5*pow(depth_u,1.6)) AS INT)
        WHEN tier_roll<cuts[3] THEN 9+CAST(FLOOR(12*pow(depth_u,1.8)) AS INT)
        ELSE 21+CAST(FLOOR(40*pow(depth_u,2.2)) AS INT) END drawn_count
    FROM drawn) d
), sourced AS (
  SELECT *,
    pmod(xxhash64('{{seed}}','source_breadth',person_index),100) breadth_roll,
    -- Source affinity: occasional guests skew to ticketing and events, regulars to POS, payments,
    -- reservations and order-at-table. Moshtix skews young. Weighted sampling without replacement.
    CASE WHEN scenario_cohort THEN transform(sequence(0,2),
        s -> element_at(array('sevenrooms','me_and_u','moshtix','momentus','pos','payments'),CAST(1+pmod(person_index*7+s*11,6) AS INT)))
      ELSE slice(transform(array_sort(transform(
        array(named_struct('s','sevenrooms','w',IF(record_count>=4,1.3,0.9)),
          named_struct('s','me_and_u','w',IF(record_count>=4,1.3,0.9)),
          named_struct('s','moshtix','w',IF(record_count>=4,0.45,1.8)*CASE WHEN age<35 THEN 1.4 WHEN age>=55 THEN 0.5 ELSE 1.0 END),
          named_struct('s','momentus','w',IF(record_count>=4,0.12,2.4)),
          named_struct('s','pos','w',IF(record_count>=4,1.5,0.6)),
          named_struct('s','payments','w',IF(record_count>=4,1.4,0.6))),
        x -> named_struct('k',-ln((pmod(xxhash64('{{seed}}','source_pref',person_index,x.s),1000000)+0.5)/1000000)/x.w,'s',x.s))),
        x -> x.s),1,3) END preferred_sources
  FROM counted
), timed AS (
  SELECT *,
    CASE WHEN scenario_cohort THEN 3
      WHEN record_count=1 THEN 1
      WHEN record_count=2 THEN IF(breadth_roll<12,1,2)
      WHEN record_count=3 THEN CASE WHEN breadth_roll<8 THEN 1 WHEN breadth_roll<48 THEN 2 ELSE 3 END
      ELSE CASE WHEN breadth_roll<15 THEN 1 WHEN breadth_roll<55 THEN 2 ELSE 3 END END source_breadth,
    -- Relationship window inside 2019-2025; longer for guests with more records.
    CASE WHEN scenario_cohort OR record_count=1 THEN 0
      WHEN record_count<=3 THEN 30+pmod(xxhash64('{{seed}}','relationship_span',person_index),1066)
      WHEN record_count<=8 THEN 365+pmod(xxhash64('{{seed}}','relationship_span',person_index),1462)
      ELSE 1095+pmod(xxhash64('{{seed}}','relationship_span',person_index),1461) END drawn_span_days
  FROM sourced
), spanned AS (
  SELECT *,least(drawn_span_days,2555-adult_from_day) span_days FROM timed
), windowed AS (
  SELECT *,
    UNIX_TIMESTAMP('2019-01-01 00:00:00')
      +(adult_from_day+pmod(xxhash64('{{seed}}','relationship_start',person_index),2556-adult_from_day-span_days))*86400 start_secs,
    span_days*86400+86399 span_secs,
    -- Low-rate contact drift, only for general-population guests seen more than once.
    NOT scenario_cohort AND record_count>=2 drift_eligible
  FROM spanned
), drift AS (
  SELECT w.*,
    drift_eligible AND home_country='AU'
      AND pmod(xxhash64('{{seed}}','drift_phone',person_index),1000)<IF(record_count>=4,70,30) phone_drift,
    drift_eligible AND pmod(xxhash64('{{seed}}','drift_email',person_index),1000)<IF(record_count>=4,60,20) email_drift,
    drift_eligible AND home_country='AU'
      AND pmod(xxhash64('{{seed}}','drift_address',person_index),1000)<IF(record_count>=4,90,40) address_drift,
    start_secs+CAST(span_secs*(0.3+pmod(xxhash64('{{seed}}','drift_phone_at',person_index),500)/1000.0) AS BIGINT) phone_at,
    start_secs+CAST(span_secs*(0.3+pmod(xxhash64('{{seed}}','drift_email_at',person_index),500)/1000.0) AS BIGINT) email_at,
    start_secs+CAST(span_secs*(0.3+pmod(xxhash64('{{seed}}','drift_address_at',person_index),500)/1000.0) AS BIGINT) address_at,
    -- A previous mobile from 04 prefixes that current numbers never use, so it cannot collide with anyone's current mobile.
    concat('+61 4',element_at(array('94','95','96','97','98'),CAST(1+pmod(xxhash64('{{seed}}','previous_mobile_prefix',person_index),5) AS INT)),' ',
      substr(lpad(CAST(pmod(person_index*611953+7919,1000000) AS STRING),6,'0'),1,3),' ',
      substr(lpad(CAST(pmod(person_index*611953+7919,1000000) AS STRING),6,'0'),4,3)) previous_mobile,
    split(personal_email,'@')[0] email_local,split(personal_email,'@')[1] email_domain,
    IF(age>=45,array('bigpond.com','optusnet.com.au','hotmail.com','yahoo.com.au','iinet.net.au'),
      array('hotmail.com','yahoo.com.au','live.com.au','outlook.com','gmail.com')) previous_domains,
    CAST(1+pmod(xxhash64('{{seed}}','previous_email_domain',person_index),5) AS INT) previous_domain_ix
  FROM windowed w
), email_candidates AS (
  SELECT *,concat(email_local,'@',IF(element_at(previous_domains,previous_domain_ix)=email_domain,
      element_at(previous_domains,1+pmod(previous_domain_ix,5)),element_at(previous_domains,previous_domain_ix))) previous_email_candidate
  FROM drift
), email_checked AS (
  -- A previous address never reuses anyone else's current or previous mailbox.
  SELECT e.*,
    IF(c.personal_email IS NOT NULL OR count(*) OVER (PARTITION BY e.previous_email_candidate)>1,
      concat(e.email_local,CAST(10+pmod(xxhash64('{{seed}}','previous_email_tail',e.person_index),89) AS STRING),'@',
        split(e.previous_email_candidate,'@')[1]),
      e.previous_email_candidate) previous_email
  FROM email_candidates e
  LEFT JOIN {{catalog}}.{{truth_schema}}.canonical_people c ON c.personal_email=e.previous_email_candidate
), moved_home AS (
  SELECT e.*,l.suburb previous_suburb,l.state previous_state,l.postcode previous_postcode,
    concat(IF(pmod(xxhash64('{{seed}}','previous_unit',e.person_index),100)<coalesce(l.apartment_pct,40),
        concat(CAST(1+pmod(xxhash64('{{seed}}','previous_unit_no',e.person_index),14) AS STRING),'/'),''),
      CAST(1+pmod(xxhash64('{{seed}}','previous_street_no',e.person_index),120) AS STRING),' ',
      IF(sn.has_type,sn.street_name,concat(sn.street_name,' ',st.street_type))) previous_line1
  FROM email_checked e
  LEFT JOIN _pick_home_locality l
    ON e.address_drift AND l.culture=e.name_culture
      AND l.bucket=CAST(pmod(xxhash64('{{seed}}','previous_locality',e.person_index),10000000) DIV 100000 AS INT)
      AND pmod(xxhash64('{{seed}}','previous_locality',e.person_index),10000000) BETWEEN l.lo AND l.hi-1
  LEFT JOIN {{catalog}}.{{reference_schema}}.street_names sn
    ON e.address_drift AND pmod(xxhash64('{{seed}}','previous_street',e.person_index),10000000) BETWEEN sn.lo AND sn.hi-1
  LEFT JOIN {{catalog}}.{{reference_schema}}.street_types st
    ON e.address_drift AND pmod(xxhash64('{{seed}}','previous_street_type',e.person_index),10000000) BETWEEN st.lo AND st.hi-1
)
SELECT m.person_index,m.truth_person_id,m.visit_segment,m.record_count,m.preferred_sources,
  least(m.source_breadth,m.record_count) source_breadth,m.start_secs,m.span_secs,
  IF(m.phone_drift,timestamp_seconds(m.phone_at),NULL) personal_phone_changed_at,IF(m.phone_drift,m.previous_mobile,NULL) previous_personal_phone,
  IF(m.email_drift,timestamp_seconds(m.email_at),NULL) personal_email_changed_at,IF(m.email_drift,m.previous_email,NULL) previous_personal_email,
  IF(m.address_drift AND m.previous_suburb IS NOT NULL,timestamp_seconds(m.address_at),NULL) home_address_changed_at,
  IF(m.address_drift AND m.previous_suburb IS NOT NULL,m.previous_line1,NULL) previous_home_address_line1,
  IF(m.address_drift AND m.previous_suburb IS NOT NULL,m.previous_suburb,NULL) previous_home_suburb,
  IF(m.address_drift AND m.previous_suburb IS NOT NULL,m.previous_state,NULL) previous_home_state,
  IF(m.address_drift AND m.previous_suburb IS NOT NULL,m.previous_postcode,NULL) previous_home_postcode,
  -- Employer changes come from employment_history; scenario cohorts keep their current employer in every record.
  IF(NOT m.scenario_cohort AND prev.company_name IS NOT NULL,CAST(cur.valid_from AS TIMESTAMP),NULL) employer_changed_at,
  IF(NOT m.scenario_cohort,prev.company_name,NULL) previous_company_name,
  IF(NOT m.scenario_cohort,prev.work_email,NULL) previous_work_email,
  IF(NOT m.scenario_cohort,prev.work_phone,NULL) previous_work_phone,
  IF(NOT m.scenario_cohort,prev.work_address_line1,NULL) previous_work_address_line1,
  IF(NOT m.scenario_cohort,prev.work_suburb,NULL) previous_work_suburb,
  IF(NOT m.scenario_cohort,prev.work_state,NULL) previous_work_state,
  IF(NOT m.scenario_cohort,prev.work_postcode,NULL) previous_work_postcode
FROM moved_home m
LEFT JOIN {{catalog}}.{{truth_schema}}.employment_history prev
  ON prev.truth_person_id=m.truth_person_id AND prev.valid_to IS NOT NULL
LEFT JOIN {{catalog}}.{{truth_schema}}.employment_history cur
  ON cur.truth_person_id=m.truth_person_id AND cur.valid_to IS NULL AND prev.truth_person_id IS NOT NULL;

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW _nickname_options AS
SELECT formal_name,collect_list(nickname) nicknames FROM {{catalog}}.{{reference_schema}}.nicknames GROUP BY formal_name;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}._identity_records_with_truth
USING DELTA COMMENT 'RESTRICTED generator staging; never grant to resolver' AS
WITH slotted_raw AS (
  SELECT g.*,slot,
    CASE WHEN g.person_index=60 AND slot IN (0,1) THEN 'sevenrooms'
      -- Each preferred source is visited once, then records concentrate on the favourite systems.
      WHEN slot<g.source_breadth THEN element_at(g.preferred_sources,slot+1)
      ELSE element_at(g.preferred_sources,CASE g.source_breadth WHEN 1 THEN 1
        WHEN 2 THEN IF(pmod(xxhash64('{{seed}}','source_slot',g.person_index,slot),100)<65,1,2)
        ELSE CASE WHEN pmod(xxhash64('{{seed}}','source_slot',g.person_index,slot),100)<55 THEN 1
          WHEN pmod(xxhash64('{{seed}}','source_slot',g.person_index,slot),100)<85 THEN 2 ELSE 3 END END)
    END source_system,
    -- Scenario cohorts keep the v4 2024-2025 window; everyone else is spread chronologically by slot.
    IF(g.person_index<3300,
      timestamp_seconds(UNIX_TIMESTAMP('2024-01-01 00:00:00')+pmod(xxhash64('{{seed}}','updated',g.person_index,slot),63072000)),
      timestamp_seconds(g.start_secs+CAST(g.span_secs*(slot+pmod(xxhash64('{{seed}}','record_ts',g.person_index,slot),1000)/1000.0)/g.record_count AS BIGINT)))
      v4_or_timeline_ts
  FROM {{catalog}}.{{truth_schema}}.guest_relationships g
  LATERAL VIEW explode(sequence(0,g.record_count-1)) slots AS slot
), slotted AS (
  -- A few young scenario-cohort guests would otherwise appear before their 18th birthday.
  SELECT s.*,IF(s.person_index<3300,greatest(s.v4_or_timeline_ts,
      timestamp_seconds(UNIX_TIMESTAMP(CAST(add_months(c.date_of_birth,216) AS TIMESTAMP))
        +pmod(xxhash64('{{seed}}','adult_ts',s.person_index,s.slot),
          -- Within the first year of adulthood, never past the end of the window; a no-op for older guests.
          greatest(1L,least(31536000L,UNIX_TIMESTAMP('2025-12-31 23:59:59')-UNIX_TIMESTAMP(CAST(add_months(c.date_of_birth,216) AS TIMESTAMP))))))),
    s.v4_or_timeline_ts) record_ts
  FROM slotted_raw s
  JOIN {{catalog}}.{{truth_schema}}.canonical_people c ON c.person_index=s.person_index
), drifted AS (
  SELECT p.* EXCEPT (personal_email,personal_phone,home_address_line1,home_suburb,home_state,home_postcode,
      company_name,work_email,work_phone,work_address_line1,work_suburb,work_state,work_postcode),
    s.slot,s.source_system,s.record_ts,s.record_count,
    coalesce(s.record_ts<s.personal_phone_changed_at,false) previous_phone,
    coalesce(s.record_ts<s.personal_email_changed_at,false) previous_email,
    coalesce(s.record_ts<s.home_address_changed_at,false) previous_address,
    coalesce(s.record_ts<s.employer_changed_at,false) previous_employer,
    -- A record captured before a change keeps the value the guest gave at the time.
    IF(coalesce(s.record_ts<s.personal_email_changed_at,false),s.previous_personal_email,p.personal_email) personal_email,
    IF(coalesce(s.record_ts<s.personal_phone_changed_at,false),s.previous_personal_phone,p.personal_phone) personal_phone,
    IF(coalesce(s.record_ts<s.home_address_changed_at,false),s.previous_home_address_line1,p.home_address_line1) home_address_line1,
    IF(coalesce(s.record_ts<s.home_address_changed_at,false),s.previous_home_suburb,p.home_suburb) home_suburb,
    IF(coalesce(s.record_ts<s.home_address_changed_at,false),s.previous_home_state,p.home_state) home_state,
    IF(coalesce(s.record_ts<s.home_address_changed_at,false),s.previous_home_postcode,p.home_postcode) home_postcode,
    IF(coalesce(s.record_ts<s.employer_changed_at,false),s.previous_company_name,p.company_name) company_name,
    IF(coalesce(s.record_ts<s.employer_changed_at,false),s.previous_work_email,p.work_email) work_email,
    IF(coalesce(s.record_ts<s.employer_changed_at,false),s.previous_work_phone,p.work_phone) work_phone,
    IF(coalesce(s.record_ts<s.employer_changed_at,false),s.previous_work_address_line1,p.work_address_line1) work_address_line1,
    IF(coalesce(s.record_ts<s.employer_changed_at,false),s.previous_work_suburb,p.work_suburb) work_suburb,
    IF(coalesce(s.record_ts<s.employer_changed_at,false),s.previous_work_state,p.work_state) work_state,
    IF(coalesce(s.record_ts<s.employer_changed_at,false),s.previous_work_postcode,p.work_postcode) work_postcode
  FROM {{catalog}}.{{truth_schema}}.canonical_people p
  JOIN slotted s ON s.person_index=p.person_index
), expanded AS (
  SELECT p.*,
    filter(array(IF(previous_phone,'previous_personal_phone',NULL),IF(previous_email,'previous_personal_email',NULL),
      IF(previous_address,'previous_home_address',NULL),IF(previous_employer,'previous_employer',NULL)),x -> x IS NOT NULL) drift_tags,
    em.domain employer_domain,
    -- Household-shared-contact pairs give the same household mobile in every system.
    IF(p.person_index BETWEEN 1400 AND 1799,IF(p.home_country<>'AU',
      first_value(p.personal_phone) OVER (PARTITION BY p.truth_household_id ORDER BY p.member_ordinal),concat('+61 4',
      substr(lpad(CAST(pmod(xxhash64('{{seed}}','shared_household_phone',p.truth_household_id),100000000) AS STRING),8,'0'),1,2),' ',
      substr(lpad(CAST(pmod(xxhash64('{{seed}}','shared_household_phone',p.truth_household_id),100000000) AS STRING),8,'0'),3,3),' ',
      substr(lpad(CAST(pmod(xxhash64('{{seed}}','shared_household_phone',p.truth_household_id),100000000) AS STRING),8,'0'),6,3))),
      p.personal_phone) effective_phone,
    -- Recycled work-contact pairs inherit one direct line at their shared employer.
    IF(p.person_index BETWEEN 2000 AND 2199 AND p.work_phone IS NOT NULL,concat('+61 2 9',
      lpad(CAST(pmod(xxhash64('{{seed}}','recycled_phone',CAST((p.person_index-2000)/2 AS BIGINT)),1000) AS STRING),3,'0'),' ',
      lpad(CAST(pmod(xxhash64('{{seed}}','recycled_phone_line',CAST((p.person_index-2000)/2 AS BIGINT)),10000) AS STRING),4,'0')),
      p.work_phone) effective_work_phone
  FROM drifted p
  LEFT JOIN {{catalog}}.{{reference_schema}}.employers em ON em.company_name=p.company_name
), base AS (
  SELECT *,sha2(concat('{{seed}}',':record:',source_system,':',person_index,':',slot),256) source_record_id,
    pmod(xxhash64('{{seed}}','variant',person_index,slot),100) variant,
    CASE WHEN person_index=10 THEN 'hangul_english'
      WHEN person_index IN (20,22) THEN 'corporate_shared_contact'
      WHEN person_index IN (30,31) THEN 'household_parent_child'
      WHEN person_index=40 THEN 'default_dob'
      WHEN person_index=50 THEN 'typo_sparse_booking'
      WHEN person_index=60 THEN 'within_source_duplicate'
      WHEN person_index=70 THEN 'english_chinese_name'
      WHEN person_index BETWEEN 400 AND 799 THEN 'source_default_dob_population'
      WHEN person_index BETWEEN 1000 AND 1399 THEN 'corporate_organiser'
      WHEN person_index BETWEEN 1400 AND 1799 THEN 'household_shared_contact'
      WHEN person_index BETWEEN 1800 AND 1999 THEN 'twins_similar_identity'
      WHEN person_index BETWEEN 2000 AND 2199 THEN 'recycled_work_contact'
      WHEN person_index BETWEEN 2200 AND 2399 THEN 'shared_payment_instrument'
      WHEN person_index BETWEEN 2400 AND 2599 THEN 'venue_placeholder'
      WHEN person_index BETWEEN 2600 AND 2999 THEN 'common_name_collision'
      WHEN person_index BETWEEN 3000 AND 3299 THEN 'context_shift_true_match'
      ELSE 'general_population' END scenario_type,
    -- Per-source capture profile (per mille). Each source asks for different fields.
    pmod(xxhash64('{{seed}}','cap_given',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 995 WHEN 'me_and_u' THEN 985
      WHEN 'moshtix' THEN 998 WHEN 'momentus' THEN 995 WHEN 'pos' THEN 965 ELSE 985 END cap_given,
    pmod(xxhash64('{{seed}}','cap_family',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 985 WHEN 'me_and_u' THEN 860
      WHEN 'moshtix' THEN 997 WHEN 'momentus' THEN 990 WHEN 'pos' THEN 930 ELSE 980 END cap_family,
    pmod(xxhash64('{{seed}}','cap_personal_email',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 900 WHEN 'me_and_u' THEN 830
      WHEN 'moshtix' THEN 975 WHEN 'momentus' THEN 450 WHEN 'pos' THEN 500 ELSE 320 END cap_personal_email,
    pmod(xxhash64('{{seed}}','cap_personal_phone',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 950 WHEN 'me_and_u' THEN 600
      WHEN 'moshtix' THEN 880 WHEN 'momentus' THEN 480 WHEN 'pos' THEN 650 ELSE 60 END cap_personal_phone,
    pmod(xxhash64('{{seed}}','cap_dob',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 420 WHEN 'me_and_u' THEN 60
      WHEN 'moshtix' THEN 900 WHEN 'momentus' THEN 300 WHEN 'pos' THEN 260 ELSE 0 END cap_dob,
    pmod(xxhash64('{{seed}}','cap_home_line',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 160 WHEN 'me_and_u' THEN 30
      WHEN 'moshtix' THEN 450 WHEN 'momentus' THEN 300 WHEN 'pos' THEN 120 ELSE 550 END cap_home_line,
    pmod(xxhash64('{{seed}}','cap_home_locality',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 260 WHEN 'me_and_u' THEN 60
      WHEN 'moshtix' THEN 920 WHEN 'momentus' THEN 380 WHEN 'pos' THEN 420 ELSE 740 END cap_home_locality,
    pmod(xxhash64('{{seed}}','cap_postcode_only',person_index,slot),1000)<CASE source_system WHEN 'pos' THEN 450 WHEN 'payments' THEN 350
      WHEN 'moshtix' THEN 250 ELSE 0 END cap_postcode_only,
    -- A work block (company plus business contact) is captured together, only for employed guests.
    pmod(xxhash64('{{seed}}','cap_company',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 120 WHEN 'momentus' THEN 720
      WHEN 'pos' THEN 50 WHEN 'payments' THEN 100 ELSE 0 END cap_company,
    -- Sources that keep a company field also keep the business email with it; payments cards carry only the company.
    source_system IN ('sevenrooms','momentus','pos') cap_work_email,
    pmod(xxhash64('{{seed}}','cap_work_phone',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 350 WHEN 'momentus' THEN 700
      WHEN 'pos' THEN 150 ELSE 0 END cap_work_phone,
    pmod(xxhash64('{{seed}}','cap_work_address',person_index,slot),1000)<CASE source_system WHEN 'momentus' THEN 650 ELSE 0 END cap_work_address,
    -- Low-rate, realistic data-entry noise.
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','given_typo',person_index,slot),1000)<CASE source_system WHEN 'payments' THEN 3 WHEN 'moshtix' THEN 7 ELSE 11 END given_typo,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','family_typo',person_index,slot),1000)<CASE source_system WHEN 'payments' THEN 4 WHEN 'moshtix' THEN 9 ELSE 14 END family_typo,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','nickname',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 70 WHEN 'me_and_u' THEN 110
      WHEN 'pos' THEN 60 WHEN 'moshtix' THEN 25 ELSE 10 END use_nickname,
    pmod(xxhash64('{{seed}}','name_case',person_index,slot),1000) name_case_roll,
    pmod(xxhash64('{{seed}}','typo_kind',person_index,slot),4) typo_kind,
    pmod(xxhash64('{{seed}}','typo_position',person_index,slot),1000) typo_position,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','email_case',person_index,slot),1000)<CASE source_system WHEN 'sevenrooms' THEN 18 WHEN 'moshtix' THEN 12 ELSE 5 END email_case,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','email_domain_typo',person_index,slot),1000)<4 email_domain_typo,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','phone_typo',person_index,slot),1000)<4 phone_typo,
    pmod(xxhash64('{{seed}}','phone_format',person_index,slot),100) phone_format_roll,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','dob_swap',person_index,slot),1000)<CASE source_system WHEN 'moshtix' THEN 4 WHEN 'pos' THEN 5 WHEN 'sevenrooms' THEN 3 ELSE 0 END dob_swap,
    pmod(xxhash64('{{seed}}','address_style',person_index,slot),100) address_style_roll,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','street_typo',person_index,slot),1000)<6 street_typo,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','suburb_typo',person_index,slot),1000)<5 suburb_typo,
    person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) AND pmod(xxhash64('{{seed}}','postcode_mismatch',person_index,slot),1000)<4 postcode_mismatch,
    pmod(xxhash64('{{seed}}','company_variant',person_index,slot),100) company_variant_roll
  FROM expanded
), noised AS (
  SELECT b.*,
    element_at(n.nicknames,CAST(1+pmod(xxhash64('{{seed}}','nickname_pick',person_index,slot),size(n.nicknames)) AS INT)) nickname,
    CAST(2+pmod(typo_position,greatest(length(given_name)-2,1)) AS INT) given_typo_at,
    CAST(2+pmod(typo_position,greatest(length(family_name)-2,1)) AS INT) family_typo_at,
    regexp_replace(effective_phone,'[^0-9]','') phone_digits,
    regexp_replace(effective_work_phone,'[^0-9]','') work_phone_digits,
    CASE WHEN home_country='AU' THEN regexp_extract(home_address_line1,'^(?:([0-9]+)/)?(.*)$',1) END unit_part,
    CASE WHEN home_country='AU' THEN regexp_extract(home_address_line1,'^(?:([0-9]+)/)?(.*)$',2) ELSE home_address_line1 END street_part
  FROM base b
  LEFT JOIN _nickname_options n ON n.formal_name=b.given_name
), typed AS (
  SELECT *,
    -- Typo shapes: adjacent transposition, dropped letter, doubled letter, vowel slip.
    CASE WHEN NOT given_typo OR length(given_name)<4 THEN given_name
      WHEN typo_kind=0 THEN concat(substr(given_name,1,given_typo_at-1),substr(given_name,given_typo_at+1,1),substr(given_name,given_typo_at,1),substr(given_name,given_typo_at+2))
      WHEN typo_kind=1 THEN concat(substr(given_name,1,given_typo_at-1),substr(given_name,given_typo_at+1))
      WHEN typo_kind=2 THEN concat(substr(given_name,1,given_typo_at),substr(given_name,given_typo_at))
      ELSE concat(substr(given_name,1,given_typo_at-1),translate(substr(given_name,given_typo_at,1),'aeiou','eiauo'),substr(given_name,given_typo_at+1)) END given_noisy,
    CASE WHEN NOT family_typo OR length(family_name)<4 THEN family_name
      WHEN typo_kind=0 THEN concat(substr(family_name,1,family_typo_at-1),substr(family_name,family_typo_at+1,1),substr(family_name,family_typo_at,1),substr(family_name,family_typo_at+2))
      WHEN typo_kind=1 THEN concat(substr(family_name,1,family_typo_at-1),substr(family_name,family_typo_at+1))
      WHEN typo_kind=2 THEN concat(substr(family_name,1,family_typo_at),substr(family_name,family_typo_at))
      ELSE concat(substr(family_name,1,family_typo_at-1),translate(substr(family_name,family_typo_at,1),'aeiou','eiauo'),substr(family_name,family_typo_at+1)) END family_noisy
  FROM noised
), shaped AS (
  SELECT *,CASE WHEN scenario_type='hangul_english' AND slot=0 THEN '민준'
    WHEN scenario_type='hangul_english' AND slot=1 THEN 'Minjun'
    WHEN scenario_type='hangul_english' AND slot=2 THEN 'Min-jun'
    WHEN scenario_type='corporate_shared_contact' AND person_index=20 THEN 'Alice'
    WHEN scenario_type='corporate_shared_contact' AND person_index=22 THEN 'Marcus'
    WHEN scenario_type='household_parent_child' AND person_index=30 THEN 'James'
    WHEN scenario_type='household_parent_child' AND person_index=31 THEN 'Jasmine'
    WHEN scenario_type='default_dob' THEN 'Sophie'
    WHEN scenario_type='typo_sparse_booking' AND slot<>2 THEN 'Matilda'
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN 'Mtailda'
    WHEN scenario_type='within_source_duplicate' THEN 'Olivia'
    WHEN scenario_type='english_chinese_name' AND slot=0 THEN 'Emily'
    WHEN scenario_type='english_chinese_name' AND slot=1 THEN 'Yutong'
    WHEN scenario_type='english_chinese_name' AND slot=2 THEN '雨桐'
    WHEN NOT cap_given THEN NULL
    WHEN use_nickname AND nickname IS NOT NULL THEN nickname
    ELSE given_noisy END observed_given,
    CASE WHEN scenario_type='hangul_english' AND slot=0 THEN '김'
      WHEN scenario_type='hangul_english' THEN 'Kim'
      WHEN scenario_type='corporate_shared_contact' AND person_index=20 THEN 'Howard'
      WHEN scenario_type='corporate_shared_contact' AND person_index=22 THEN 'Reed'
      WHEN scenario_type='household_parent_child' THEN 'Walker'
      WHEN scenario_type='default_dob' THEN 'Martin'
      WHEN scenario_type='typo_sparse_booking' AND slot<>2 THEN 'Wilson'
      WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN 'Wlison'
      WHEN scenario_type='within_source_duplicate' THEN 'Brown'
      WHEN scenario_type='english_chinese_name' AND slot=2 THEN '张'
      WHEN scenario_type='english_chinese_name' THEN 'Zhang'
      WHEN NOT cap_family THEN NULL
      ELSE family_noisy END observed_family,
    scenario_type IN ('corporate_shared_contact','household_parent_child','typo_sparse_booking','within_source_duplicate',
      'english_chinese_name','hangul_english','default_dob') OR (scenario_type='context_shift_true_match' AND person_index=3000) pinned_hero,
    -- Personal email as each source records it (canonical address, occasional case or domain slip).
    CASE WHEN NOT email_case AND NOT email_domain_typo THEN personal_email
      WHEN email_domain_typo THEN concat(split(personal_email,'@')[0],'@',CASE split(personal_email,'@')[1]
        WHEN 'gmail.com' THEN element_at(array('gmial.com','gmail.con','gmai.com','gmail.co'),CAST(1+pmod(typo_position,4) AS INT))
        WHEN 'hotmail.com' THEN element_at(array('hotmial.com','hotmail.co','hotmal.com'),CAST(1+pmod(typo_position,3) AS INT))
        WHEN 'outlook.com' THEN 'outlok.com' WHEN 'icloud.com' THEN 'iclould.com' WHEN 'yahoo.com' THEN 'yaho.com'
        WHEN 'bigpond.com' THEN 'bigpond.con' ELSE split(personal_email,'@')[1] END)
      ELSE concat(upper(substr(personal_email,1,1)),substr(personal_email,2)) END personal_email_observed,
    CASE WHEN phone_digits IS NULL OR NOT (phone_digits LIKE '614%' AND length(phone_digits)=11) THEN effective_phone
      ELSE CASE
        WHEN source_system IN ('sevenrooms','payments') THEN concat('+',phone_digits)
        WHEN source_system='momentus' THEN effective_phone
        WHEN source_system='moshtix' OR (source_system='pos' AND phone_format_roll<30) THEN concat('0',substr(phone_digits,3))
        ELSE concat('0',substr(phone_digits,3,3),' ',substr(phone_digits,6,3),' ',substr(phone_digits,9,3)) END END personal_phone_formatted,
    CASE WHEN effective_work_phone IS NULL THEN NULL
      WHEN source_system='momentus' THEN concat('(0',substr(work_phone_digits,3,1),') ',substr(work_phone_digits,4,4),' ',substr(work_phone_digits,8,4))
      WHEN source_system='sevenrooms' THEN concat('+',work_phone_digits)
      ELSE concat('0',substr(work_phone_digits,3,1),' ',substr(work_phone_digits,4,4),' ',substr(work_phone_digits,8,4)) END work_phone_formatted,
    CASE WHEN home_address_line1 IS NULL THEN NULL
      WHEN home_country<>'AU' THEN home_address_line1
      ELSE concat(
        CASE WHEN unit_part='' THEN ''
          WHEN source_system IN ('sevenrooms','momentus') AND address_style_roll<15 THEN concat('Unit ',unit_part,', ')
          WHEN source_system='pos' AND address_style_roll<8 THEN concat('U',unit_part,' ')
          ELSE concat(unit_part,'/') END,
        CASE WHEN source_system IN ('pos','payments','me_and_u') OR (source_system='momentus' AND address_style_roll<50) THEN
          regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
            regexp_replace(regexp_replace(regexp_replace(street_part,' Street$',' St'),' Road$',' Rd'),' Avenue$',' Ave'),' Parade$',' Pde'),
            ' Crescent$',' Cres'),' Place$',' Pl'),' Drive$',' Dr'),' Terrace$',' Tce'),' Close$',' Cl'),' Court$',' Ct'),' Lane$',' Ln'),' Highway$',' Hwy')
          ELSE street_part END) END address_formatted
  FROM typed
), assembled AS (
  SELECT *,
    CASE WHEN scenario_type='hangul_english' AND slot=0 THEN NULL
      WHEN scenario_type='hangul_english' THEN 'minjun.kim@gmail.com'
      WHEN scenario_type='corporate_shared_contact' AND person_index=20 THEN 'alice.howard@gmail.com'
      WHEN scenario_type='corporate_shared_contact' AND person_index=22 THEN 'marcus.reed@outlook.com'
      WHEN scenario_type='household_parent_child' AND person_index=30 THEN 'james.walker@gmail.com'
      WHEN scenario_type='household_parent_child' AND person_index=31 THEN 'jasmine.walker@hotmail.com'
      WHEN scenario_type='typo_sparse_booking' AND slot<>2 THEN 'matilda.wilson@gmail.com'
      WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
      WHEN scenario_type='within_source_duplicate' THEN 'olivia.brown@gmail.com'
      WHEN scenario_type='english_chinese_name' AND slot=0 THEN 'emily.zhang@icloud.com'
      WHEN scenario_type='english_chinese_name' AND slot IN (1,2) THEN 'yutong.zhang@hotmail.com'
      WHEN scenario_type='context_shift_true_match' AND slot=0 THEN NULL
      WHEN scenario_type='context_shift_true_match' AND slot=2 THEN IF(person_index=3000,'samt2000@hotmail.com',
        lower(concat(regexp_replace(given_name,'[^A-Za-z]',''),substr(family_name,1,1),
          IF(pmod(person_index,3)=0,CAST(year(date_of_birth) AS STRING),substr(CAST(year(date_of_birth) AS STRING),3,2)),'@',
          element_at(array('hotmail.com','yahoo.com.au','bigpond.com','outlook.com','hotmail.com','optusnet.com.au'),CAST(1+pmod(person_index,6) AS INT)))))
      WHEN scenario_type='corporate_organiser' AND employer_domain IS NOT NULL THEN concat(
        element_at(array('events','bookings','reception','office','admin','ea'),CAST(1+pmod(CAST(FLOOR((person_index-1000)/10) AS BIGINT),6) AS INT)),'@',employer_domain)
      WHEN scenario_type='venue_placeholder' THEN 'noemail@venue.example'
      WHEN scenario_type='default_dob' THEN personal_email
      WHEN NOT cap_personal_email THEN NULL
      ELSE personal_email_observed END personal_email_out,
    cap_company AND company_name IS NOT NULL AND scenario_type NOT IN ('household_parent_child','typo_sparse_booking','english_chinese_name','hangul_english')
      OR (scenario_type='recycled_work_contact' AND company_name IS NOT NULL)
      OR (scenario_type='context_shift_true_match' AND slot=0 AND company_name IS NOT NULL) work_block
  FROM shaped
), finalised AS (
  SELECT *,
    CASE WHEN scenario_type='corporate_shared_contact' THEN 'events@pacificevents.com.au'
      WHEN scenario_type IN ('household_parent_child','typo_sparse_booking','english_chinese_name','hangul_english') THEN NULL
      WHEN scenario_type='context_shift_true_match' AND slot=0 THEN work_email
      WHEN scenario_type='recycled_work_contact' AND employer_domain IS NOT NULL THEN concat(
        element_at(array('events.coordinator','office.manager','partnerships','marketing','accounts','reception'),CAST(1+pmod(CAST(FLOOR((person_index-2000)/2) AS BIGINT),6) AS INT)),'@',employer_domain)
      WHEN work_block AND cap_work_email THEN work_email ELSE NULL END work_email_out,
    CASE WHEN scenario_type='corporate_shared_contact' THEN 'Pacific Events'
      WHEN NOT work_block THEN NULL
      WHEN source_system='momentus' AND company_variant_roll<10 THEN concat(company_name,' Pty Ltd')
      WHEN source_system='sevenrooms' AND company_variant_roll<5 THEN upper(company_name)
      ELSE company_name END company_out
  FROM assembled
)
SELECT source_system,source_record_id,truth_person_id,truth_household_id,person_index,slot,scenario_type,
  personal_email_origin canonical_personal_email_origin,
  filter(array(
    IF(given_typo AND length(given_name)>=4 AND cap_given AND NOT (use_nickname AND nickname IS NOT NULL),'given_name_typo',NULL),
    IF(use_nickname AND nickname IS NOT NULL AND cap_given,'given_name_nickname',NULL),
    IF(family_typo AND length(family_name)>=4 AND cap_family,'family_name_typo',NULL),
    IF((email_case OR email_domain_typo) AND cap_personal_email AND personal_email_out=personal_email_observed,'personal_email_variant',NULL),
    IF(phone_typo AND home_country='AU' AND cap_personal_phone AND scenario_type<>'household_shared_contact' AND NOT (scenario_type='context_shift_true_match' AND slot=2),'personal_phone_typo',NULL),
    IF(dob_swap AND cap_dob AND day(date_of_birth)<=12 AND day(date_of_birth)<>month(date_of_birth) AND scenario_type<>'twins_similar_identity' AND NOT (scenario_type='source_default_dob_population' AND source_system='momentus'),'dob_day_month_swap',NULL),
    IF(street_typo AND cap_home_line AND home_country='AU' AND NOT (scenario_type='context_shift_true_match' AND slot=2),'street_typo',NULL),
    IF(suburb_typo AND (cap_home_line OR (cap_home_locality AND NOT cap_postcode_only)) AND home_country='AU' AND NOT (scenario_type='context_shift_true_match' AND slot=2),'suburb_typo',NULL),
    IF(postcode_mismatch AND (cap_home_line OR cap_home_locality) AND home_country='AU' AND NOT (scenario_type='context_shift_true_match' AND slot=2),'postcode_mismatch',NULL)),
    x -> x IS NOT NULL) corruption_tags,
  CASE WHEN observed_given IS NOT NULL AND source_system='payments' AND name_case_roll<750 AND person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) THEN upper(observed_given)
    WHEN observed_given IS NOT NULL AND source_system IN ('me_and_u','moshtix') AND name_case_roll<25 AND NOT pinned_hero THEN lower(observed_given)
    ELSE observed_given END given_name,
  CASE WHEN observed_family IS NOT NULL AND source_system='payments' AND name_case_roll<750 AND person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) THEN upper(observed_family)
    WHEN observed_family IS NOT NULL AND source_system IN ('me_and_u','moshtix') AND name_case_roll<25 AND NOT pinned_hero THEN lower(observed_family)
    ELSE observed_family END family_name,
  CASE WHEN scenario_type='hangul_english' AND slot=0 THEN '김민준'
    WHEN scenario_type='english_chinese_name' AND slot IN (1,2) THEN '张雨桐'
    WHEN source_system='payments' AND name_case_roll<750 AND person_index NOT IN (10,20,22,30,31,40,50,60,70,3000) THEN upper(concat_ws(' ',observed_given,observed_family))
    WHEN source_system IN ('me_and_u','moshtix') AND name_case_roll<25 AND NOT pinned_hero THEN lower(concat_ws(' ',observed_given,observed_family))
    ELSE nullif(concat_ws(' ',observed_given,observed_family),'') END full_name,
  personal_email_out personal_email,
  work_email_out work_email,
  CASE WHEN scenario_type='corporate_shared_contact' AND person_index=20 THEN '+61 401 880 020'
    WHEN scenario_type='corporate_shared_contact' AND person_index=22 THEN '+61 401 880 022'
    WHEN scenario_type='household_parent_child' THEN '+61 412 555 030'
    WHEN scenario_type='hangul_english' AND slot=0 THEN NULL
    WHEN scenario_type='hangul_english' THEN '+61 412 555 010'
    WHEN scenario_type='typo_sparse_booking' AND slot<>2 THEN '+61 412 555 050'
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN scenario_type='within_source_duplicate' THEN '+61 412 555 060'
    WHEN scenario_type='english_chinese_name' THEN '+61 412 555 070'
    WHEN scenario_type='context_shift_true_match' AND slot=2 THEN NULL
    WHEN scenario_type IN ('household_shared_contact','default_dob') THEN personal_phone_formatted
    WHEN NOT cap_personal_phone THEN NULL
    WHEN phone_typo AND home_country='AU' THEN concat(substr(personal_phone_formatted,1,length(personal_phone_formatted)-2),
      substr(personal_phone_formatted,length(personal_phone_formatted),1),substr(personal_phone_formatted,length(personal_phone_formatted)-1,1))
    ELSE personal_phone_formatted END personal_phone,
  CASE WHEN scenario_type='corporate_shared_contact' THEN '+61 2 9000 8800'
    WHEN scenario_type='hangul_english' THEN NULL
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN scenario_type='recycled_work_contact' THEN work_phone_formatted
    WHEN work_block AND cap_work_phone THEN work_phone_formatted ELSE NULL END work_phone,
  CASE WHEN scenario_type='hangul_english' THEN DATE '1991-02-14'
    WHEN scenario_type='corporate_shared_contact' AND person_index=20 THEN DATE '1988-03-11'
    WHEN scenario_type='corporate_shared_contact' AND person_index=22 THEN DATE '1990-07-22'
    WHEN scenario_type='household_parent_child' AND person_index=30 THEN DATE '1974-09-09'
    WHEN scenario_type='household_parent_child' AND person_index=31 THEN DATE '2002-12-12'
    WHEN scenario_type='default_dob' AND source_system='momentus' THEN DATE '1957-05-01'
    WHEN scenario_type='default_dob' THEN DATE '1989-11-23'
    WHEN scenario_type='source_default_dob_population' AND source_system='momentus' THEN DATE '1957-05-01'
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN scenario_type='typo_sparse_booking' THEN DATE '1994-06-18'
    WHEN scenario_type='within_source_duplicate' THEN DATE '1986-08-04'
    WHEN scenario_type='english_chinese_name' THEN DATE '1992-02-29'
    WHEN scenario_type='twins_similar_identity' THEN date_of_birth
    WHEN NOT cap_dob THEN NULL
    WHEN dob_swap AND day(date_of_birth)<=12 THEN make_date(year(date_of_birth),day(date_of_birth),month(date_of_birth))
    ELSE date_of_birth END date_of_birth,
  CASE WHEN scenario_type='hangul_english' AND slot=0 THEN '서울특별시 강남구 테헤란로 152'
    WHEN scenario_type='hangul_english' THEN '152 Teheran-ro'
    WHEN scenario_type='household_parent_child' THEN '14 Crown Street'
    WHEN scenario_type='typo_sparse_booking' AND slot<>2 THEN '7 Oxford Road'
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN '7 Oxfrd Rd'
    WHEN scenario_type='within_source_duplicate' THEN '21 Bourke Street'
    WHEN scenario_type='english_chinese_name' AND slot=0 THEN '8 Quay Street'
    WHEN scenario_type='english_chinese_name' AND slot IN (1,2) THEN '澳大利亚新南威尔士州悉尼码头街8号'
    WHEN scenario_type='context_shift_true_match' AND slot=2 THEN NULL
    WHEN NOT cap_home_line THEN NULL
    WHEN street_typo AND home_country='AU' THEN regexp_replace(IF(source_system='payments',upper(address_formatted),address_formatted),'^(.*?\\b[A-Za-z]{2})[A-Za-z]([A-Za-z]{2,})','$1$2')
    WHEN source_system='payments' THEN upper(address_formatted)
    ELSE address_formatted END home_address_line1,
  CASE WHEN scenario_type='hangul_english' AND slot=0 THEN '강남구'
    WHEN scenario_type='hangul_english' THEN 'Gangnam-gu'
    WHEN scenario_type='household_parent_child' THEN 'Surry Hills'
    WHEN scenario_type='typo_sparse_booking' AND slot<>2 THEN 'Sydney'
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN 'Sydny'
    WHEN scenario_type='within_source_duplicate' THEN 'Alexandria'
    WHEN scenario_type='english_chinese_name' AND slot=0 THEN 'Sydney'
    WHEN scenario_type='english_chinese_name' AND slot IN (1,2) THEN '悉尼'
    WHEN scenario_type='context_shift_true_match' AND slot=2 THEN NULL
    WHEN NOT (cap_home_line OR cap_home_locality) OR (cap_postcode_only AND NOT cap_home_line) THEN NULL
    WHEN suburb_typo AND home_country='AU' THEN regexp_replace(IF(source_system='payments',upper(home_suburb),home_suburb),'^(.*?[A-Za-z]{2})[A-Za-z]([A-Za-z]{2,})','$1$2')
    WHEN source_system='payments' THEN upper(home_suburb)
    ELSE home_suburb END home_suburb,
  CASE WHEN scenario_type='hangul_english' AND slot=0 THEN '서울'
    WHEN scenario_type='hangul_english' THEN 'Seoul'
    WHEN scenario_type='household_parent_child' THEN 'NSW'
    WHEN scenario_type IN ('typo_sparse_booking','within_source_duplicate') THEN 'NSW'
    WHEN scenario_type='english_chinese_name' AND slot=0 THEN 'NSW'
    WHEN scenario_type='english_chinese_name' AND slot IN (1,2) THEN '新南威尔士州'
    WHEN scenario_type='context_shift_true_match' AND slot=2 THEN NULL
    WHEN NOT (cap_home_line OR cap_home_locality) OR (cap_postcode_only AND NOT cap_home_line) THEN NULL
    ELSE home_state END home_state,
  CASE WHEN scenario_type='hangul_english' THEN '06236'
    WHEN scenario_type='household_parent_child' THEN '2010'
    WHEN scenario_type='typo_sparse_booking' THEN '2000'
    WHEN scenario_type='within_source_duplicate' THEN '2015'
    WHEN scenario_type='english_chinese_name' THEN '2000'
    WHEN scenario_type='context_shift_true_match' AND slot=2 THEN NULL
    WHEN NOT (cap_home_line OR cap_home_locality) THEN NULL
    -- Explicit low-rate corruption: a neighbouring postcode keyed against the wrong suburb.
    WHEN postcode_mismatch AND home_country='AU' THEN CAST(CAST(home_postcode AS INT)+IF(pmod(typo_position,2)=0,1,-1)*(1+pmod(typo_position,7)) AS STRING)
    ELSE home_postcode END home_postcode,
  CASE WHEN scenario_type='hangul_english' THEN 'KR'
    WHEN scenario_type IN ('household_parent_child','typo_sparse_booking','within_source_duplicate','english_chinese_name') THEN 'AU'
    WHEN scenario_type='context_shift_true_match' AND slot=2 THEN NULL
    WHEN NOT (cap_home_line OR cap_home_locality) THEN NULL
    ELSE home_country END home_country,
  CASE WHEN scenario_type='corporate_shared_contact' THEN '88 Pitt Street'
    WHEN scenario_type='hangul_english' THEN NULL
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN work_block AND cap_work_address THEN work_address_line1 ELSE NULL END work_address_line1,
  CASE WHEN scenario_type='corporate_shared_contact' THEN 'Sydney'
    WHEN scenario_type='hangul_english' THEN NULL
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN work_block AND cap_work_address THEN work_suburb ELSE NULL END work_suburb,
  CASE WHEN scenario_type='corporate_shared_contact' THEN 'NSW'
    WHEN scenario_type='hangul_english' THEN NULL
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN work_block AND cap_work_address THEN work_state ELSE NULL END work_state,
  CASE WHEN scenario_type='corporate_shared_contact' THEN '2000'
    WHEN scenario_type='hangul_english' THEN NULL
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN work_block AND cap_work_address THEN work_postcode ELSE NULL END work_postcode,
  CASE WHEN scenario_type='corporate_shared_contact' THEN 'AU'
    WHEN scenario_type='hangul_english' THEN NULL
    WHEN scenario_type='typo_sparse_booking' AND slot=2 THEN NULL
    WHEN work_block AND cap_work_address THEN 'AU' ELSE NULL END work_country,
  company_out company_name,
  record_ts source_updated_at,
  drift_tags
FROM finalised;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{source_schema}}.source_identity_records
USING DELTA COMMENT 'Blinded cross-system identity observations; contains no benchmark truth' AS
SELECT source_system,source_record_id,given_name,family_name,full_name,personal_email,work_email,personal_phone,work_phone,date_of_birth,
  home_address_line1,home_suburb,home_state,home_postcode,home_country,work_address_line1,work_suburb,work_state,work_postcode,work_country,
  company_name,source_updated_at
FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.identity_record_truth
USING DELTA COMMENT 'RESTRICTED record-to-person answer key' AS
SELECT sha2(concat_ws('|',lower(trim(source_system)),trim(source_record_id)),256) identity_record_id,
  truth_person_id,source_system,
  CASE WHEN pmod(xxhash64('{{seed}}','split',person_index),100)<70 THEN 'development'
    WHEN pmod(xxhash64('{{seed}}','split',person_index),100)<90 THEN 'standard_holdout'
    ELSE 'challenge_holdout' END evaluation_split,
  pmod(xxhash64('{{seed}}','split',person_index),100)>=70 include_in_benchmark
FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.identity_record_cohorts
USING DELTA COMMENT 'RESTRICTED benchmark cohorts' AS
SELECT sha2(concat_ws('|',lower(trim(source_system)),trim(source_record_id)),256) identity_record_id,
  'scenario_type' cohort_name,scenario_type cohort_value
FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth
UNION ALL
SELECT sha2(concat_ws('|',lower(trim(source_system)),trim(source_record_id)),256),
  'source_system',source_system
FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth
UNION ALL
SELECT sha2(concat_ws('|',lower(trim(source_system)),trim(source_record_id)),256),
  'corruption',corruption
FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth
LATERAL VIEW explode(corruption_tags) t AS corruption
UNION ALL
SELECT sha2(concat_ws('|',lower(trim(source_system)),trim(source_record_id)),256),
  'contact_drift',drift
FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth
LATERAL VIEW explode(drift_tags) t AS drift;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.curated_scenario_cases
USING DELTA COMMENT 'RESTRICTED answer key for the seven presenter scenarios; evaluator only' AS
WITH fixtures(
  scenario_key,left_person_index,left_slot,right_person_index,right_slot,
  expected_same_person,expected_decision,expected_candidate_route,required_safeguard,presenter_narrative
) AS (
  VALUES
    ('hangul_english',10L,0,10L,1,true,'AUTO_MATCH','ANN_MULTILINGUAL','MULTILINGUAL_CORROBORATION',
      'The same guest enters a Korean name and address in Hangul, then an English transliteration in another system.'),
    ('corporate_shared_contact',20L,0,22L,1,false,'REJECT','DETERMINISTIC_SHARED_CONTACT','PERSONAL_CONTRADICTION',
      'Two colleagues share an events inbox, office phone and workplace, but their personal identities contradict.'),
    ('household_parent_child',30L,0,31L,0,false,'REJECT','DETERMINISTIC_HOUSEHOLD','VALID_DOB_CONFLICT',
      'A parent and adult child share a surname, first initial, home address and phone but have different valid birthdays.'),
    ('default_dob',40L,0,40L,1,true,'AUTO_MATCH','DETERMINISTIC_PERSONAL_CONTEXT','IGNORE_SOURCE_DEFAULT_DOB',
      'One source supplies its repeated default birthday while another carries the guest’s real birthday.'),
    ('typo_sparse_booking',50L,0,50L,2,true,'REVIEW','ANN_NAME_OR_ADDRESS','INSUFFICIENT_EVIDENCE_REVIEW',
      'A sparse booking contains transposed name characters and a typo-heavy address, producing a plausible but insufficient candidate.'),
    ('within_source_duplicate',60L,0,60L,1,true,'AUTO_MATCH','WITHIN_SOURCE','WITHIN_SOURCE_ALLOWED',
      'SevenRooms contains two source-local profiles for the same guest.'),
    ('english_chinese_name',70L,0,70L,1,true,'AUTO_MATCH','ANN_ADDRESS_CONTEXT','INDEPENDENT_CONTEXT_REQUIRED',
      'Emily Zhang appears elsewhere under the Chinese legal identity Yutong Zhang / 张雨桐; address retrieval and independent facts establish the link.')
), located AS (
  SELECT f.*,l.source_system left_source_system,l.source_record_id left_source_record_id,
    r.source_system right_source_system,r.source_record_id right_source_record_id
  FROM fixtures f
  JOIN {{catalog}}.{{truth_schema}}._identity_records_with_truth l
    ON l.person_index=f.left_person_index AND l.slot=f.left_slot
  JOIN {{catalog}}.{{truth_schema}}._identity_records_with_truth r
    ON r.person_index=f.right_person_index AND r.slot=f.right_slot
)
SELECT scenario_key,
  sha2(concat_ws('|',lower(trim(left_source_system)),trim(left_source_record_id)),256) left_identity_record_id,
  sha2(concat_ws('|',lower(trim(right_source_system)),trim(right_source_record_id)),256) right_identity_record_id,
  expected_same_person,expected_decision,expected_candidate_route,required_safeguard,presenter_narrative
FROM located;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.hard_negative_cases
USING DELTA COMMENT 'RESTRICTED curated dangerous non-match answer key' AS
WITH representatives AS (
  SELECT scenario_type,person_index,truth_household_id,
    sha2(concat_ws('|',lower(trim(source_system)),trim(source_record_id)),256) identity_record_id,
    CASE
      WHEN scenario_type='corporate_organiser' THEN concat('corporate:',CAST(FLOOR((person_index-1000)/10) AS STRING))
      WHEN scenario_type='household_shared_contact' THEN concat('household:',truth_household_id)
      WHEN scenario_type='common_name_collision' THEN concat('common_name:',CAST(pmod(person_index-2600,200) AS STRING))
      ELSE concat(scenario_type,':',CAST(FLOOR(person_index/2) AS STRING)) END challenge_group
  FROM {{catalog}}.{{truth_schema}}._identity_records_with_truth
  WHERE scenario_type IN ('corporate_organiser','household_shared_contact','twins_similar_identity','recycled_work_contact','shared_payment_instrument','venue_placeholder','common_name_collision')
    AND slot=0
), candidates AS (
  SELECT scenario_type,person_index,identity_record_id left_identity_record_id,
    lead(identity_record_id) OVER (PARTITION BY scenario_type,challenge_group ORDER BY person_index) right_identity_record_id,
    lead(person_index) OVER (PARTITION BY scenario_type,challenge_group ORDER BY person_index) right_person_index,
    row_number() OVER (PARTITION BY scenario_type,challenge_group ORDER BY person_index) pair_rank
  FROM representatives
)
SELECT sha2(concat('{{seed}}',':hard-negative:',scenario_type,':',person_index),256) case_id,
  scenario_type,left_identity_record_id,right_identity_record_id,
  CASE scenario_type
    WHEN 'corporate_organiser' THEN 'Two guests booked by the same corporate organiser must remain distinct.'
    WHEN 'household_shared_contact' THEN 'Household members share contact details but are different guests.'
    WHEN 'twins_similar_identity' THEN 'Twins have near-identical demographics and must not be collapsed.'
    WHEN 'recycled_work_contact' THEN 'A work contact was reassigned after an employment change.'
    WHEN 'shared_payment_instrument' THEN 'A payment instrument is shared but ownership remains individual.'
    WHEN 'venue_placeholder' THEN 'A venue placeholder is not a stable guest identifier.'
    ELSE 'Common names and locality are insufficient evidence for a merge.' END presenter_narrative,
  false expected_same_person
FROM candidates WHERE pair_rank=1 AND right_identity_record_id IS NOT NULL AND person_index<>right_person_index;

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW _profile_ranked AS
SELECT *,row_number() OVER (PARTITION BY source_system ORDER BY source_record_id)-1 source_rank,
  count(*) OVER (PARTITION BY source_system) source_count
FROM {{catalog}}.{{source_schema}}.source_identity_records;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{source_schema}}.sevenrooms_reservations USING DELTA AS
WITH e AS (SELECT id event_index FROM range(120000)),p AS (SELECT * FROM _profile_ranked WHERE source_system='sevenrooms')
SELECT sha2(concat('{{seed}}',':sevenrooms-reservation:',e.event_index),256) reservation_id,p.source_record_id,
  CASE WHEN pmod(xxhash64('{{seed}}','venue',e.event_index),100)<35 THEN 'Venue A'
    WHEN pmod(xxhash64('{{seed}}','venue',e.event_index),100)<60 THEN 'Venue B'
    WHEN pmod(xxhash64('{{seed}}','venue',e.event_index),100)<78 THEN 'Venue C'
    WHEN pmod(xxhash64('{{seed}}','venue',e.event_index),100)<91 THEN 'Venue D' ELSE 'Venue E' END venue_name,
  timestamp_seconds(UNIX_TIMESTAMP('2024-01-01 00:00:00')+pmod(xxhash64('{{seed}}','reservation_ts',e.event_index),63072000)) reservation_at,
  1+pmod(xxhash64('{{seed}}','party_size',e.event_index),12) party_size,
  CAST(40+pmod(xxhash64('{{seed}}','booking_value',e.event_index),90)+pmod(xxhash64('{{seed}}','booking_tail',e.event_index),8)*pmod(xxhash64('{{seed}}','booking_tail2',e.event_index),65) AS DECIMAL(12,2)) booking_value_aud
FROM e JOIN p ON p.source_rank=pmod(xxhash64('{{seed}}','sevenrooms_profile',e.event_index),p.source_count);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{source_schema}}.me_and_u_orders USING DELTA AS
WITH e AS (SELECT id event_index FROM range(200000)),p AS (SELECT * FROM _profile_ranked WHERE source_system='me_and_u')
SELECT sha2(concat('{{seed}}',':me-and-u-order:',e.event_index),256) order_id,p.source_record_id,
  timestamp_seconds(UNIX_TIMESTAMP('2024-01-01 00:00:00')+pmod(xxhash64('{{seed}}','order_ts',e.event_index),63072000)) ordered_at,
  CASE WHEN pmod(xxhash64('{{seed}}','order_type',e.event_index),100)<55 THEN 'beverage'
    WHEN pmod(xxhash64('{{seed}}','order_type',e.event_index),100)<82 THEN 'food' ELSE 'food_and_beverage' END order_type,
  CAST(8+pmod(xxhash64('{{seed}}','order_value',e.event_index),45)+pmod(xxhash64('{{seed}}','order_tail',e.event_index),6)*pmod(xxhash64('{{seed}}','order_tail2',e.event_index),35) AS DECIMAL(12,2)) order_value_aud
FROM e JOIN p ON p.source_rank=pmod(xxhash64('{{seed}}','me_and_u_profile',e.event_index),p.source_count);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{source_schema}}.moshtix_tickets USING DELTA AS
WITH e AS (SELECT id event_index FROM range(80000)),p AS (SELECT * FROM _profile_ranked WHERE source_system='moshtix')
SELECT sha2(concat('{{seed}}',':moshtix-ticket:',e.event_index),256) ticket_id,p.source_record_id,
  element_at(array('Live Music','New Year’s Eve','Comedy','Food Festival'),CAST(1+pmod(xxhash64('{{seed}}','event_type',e.event_index),4) AS INT)) event_name,
  date_add(DATE '2024-01-01',CAST(pmod(xxhash64('{{seed}}','event_date',e.event_index),730) AS INT)) event_date,
  CAST(30+pmod(xxhash64('{{seed}}','ticket_value',e.event_index),270) AS DECIMAL(12,2)) ticket_value_aud
FROM e JOIN p ON p.source_rank=pmod(xxhash64('{{seed}}','moshtix_profile',e.event_index),p.source_count);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{source_schema}}.momentus_bookings USING DELTA AS
WITH e AS (SELECT id event_index FROM range(30000)),p AS (SELECT * FROM _profile_ranked WHERE source_system='momentus')
SELECT sha2(concat('{{seed}}',':momentus-booking:',e.event_index),256) booking_id,p.source_record_id,
  element_at(array('Wedding','Conference','Private Dining','Product Launch'),CAST(1+pmod(xxhash64('{{seed}}','booking_type',e.event_index),4) AS INT)) booking_type,
  date_add(DATE '2024-01-01',CAST(pmod(xxhash64('{{seed}}','booking_date',e.event_index),730) AS INT)) booking_date,
  CAST(1500+pmod(xxhash64('{{seed}}','booking_revenue',e.event_index),48500) AS DECIMAL(12,2)) booking_revenue_aud
FROM e JOIN p ON p.source_rank=pmod(xxhash64('{{seed}}','momentus_profile',e.event_index),p.source_count);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{source_schema}}.pos_transactions USING DELTA AS
WITH e AS (SELECT id event_index FROM range({{pos_rows}})),p AS (SELECT * FROM _profile_ranked WHERE source_system='pos')
SELECT sha2(concat('{{seed}}',':pos-transaction:',e.event_index),256) transaction_id,p.source_record_id,
  CASE WHEN pmod(xxhash64('{{seed}}','pos_venue',e.event_index),100)<35 THEN 'Venue A'
    WHEN pmod(xxhash64('{{seed}}','pos_venue',e.event_index),100)<60 THEN 'Venue B'
    WHEN pmod(xxhash64('{{seed}}','pos_venue',e.event_index),100)<78 THEN 'Venue C'
    WHEN pmod(xxhash64('{{seed}}','pos_venue',e.event_index),100)<91 THEN 'Venue D' ELSE 'Venue E' END venue_name,
  timestamp_seconds(UNIX_TIMESTAMP('2024-01-01 00:00:00')+pmod(xxhash64('{{seed}}','pos_ts',e.event_index),63072000)) transacted_at,
  CASE WHEN pmod(xxhash64('{{seed}}','spend_type',e.event_index),100)<58 THEN 'beverage'
    WHEN pmod(xxhash64('{{seed}}','spend_type',e.event_index),100)<84 THEN 'food' ELSE 'mixed' END spend_type,
  CAST(6+pmod(xxhash64('{{seed}}','pos_value',e.event_index),55)+pmod(xxhash64('{{seed}}','pos_tail',e.event_index),8)*pmod(xxhash64('{{seed}}','pos_tail2',e.event_index),60) AS DECIMAL(12,2)) transaction_value_aud
FROM e JOIN p ON p.source_rank=pmod(xxhash64('{{seed}}','pos_profile',e.event_index),p.source_count);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{source_schema}}.tokenized_payment_events USING DELTA AS
WITH e AS (SELECT id event_index FROM range(400000)),p AS (SELECT * FROM _profile_ranked WHERE source_system='payments')
SELECT sha2(concat('{{seed}}',':payment-event:',e.event_index),256) payment_event_id,p.source_record_id,
  sha2(concat('{{seed}}',':payment-token:',CAST(pmod(p.source_rank,22000) AS STRING)),256) payment_token,
  timestamp_seconds(UNIX_TIMESTAMP('2024-01-01 00:00:00')+pmod(xxhash64('{{seed}}','payment_ts',e.event_index),63072000)) paid_at,
  CAST(6+pmod(xxhash64('{{seed}}','payment_value',e.event_index),995) AS DECIMAL(12,2)) payment_value_aud,
  element_at(array('visa','mastercard','amex'),CAST(1+pmod(xxhash64('{{seed}}','network',e.event_index),3) AS INT)) card_network
FROM e JOIN p ON p.source_rank=pmod(xxhash64('{{seed}}','payment_profile',e.event_index),p.source_count);

-- COMMAND ----------
CREATE OR REPLACE VIEW {{catalog}}.{{source_schema}}.sevenrooms_profiles AS
SELECT * FROM {{catalog}}.{{source_schema}}.source_identity_records WHERE source_system='sevenrooms';

-- COMMAND ----------
CREATE OR REPLACE VIEW {{catalog}}.{{source_schema}}.me_and_u_profiles AS
SELECT * FROM {{catalog}}.{{source_schema}}.source_identity_records WHERE source_system='me_and_u';

-- COMMAND ----------
CREATE OR REPLACE VIEW {{catalog}}.{{source_schema}}.moshtix_profiles AS
SELECT * FROM {{catalog}}.{{source_schema}}.source_identity_records WHERE source_system='moshtix';

-- COMMAND ----------
CREATE OR REPLACE VIEW {{catalog}}.{{source_schema}}.momentus_profiles AS
SELECT * FROM {{catalog}}.{{source_schema}}.source_identity_records WHERE source_system='momentus';

-- COMMAND ----------
CREATE OR REPLACE VIEW {{catalog}}.{{source_schema}}.pos_profiles AS
SELECT * FROM {{catalog}}.{{source_schema}}.source_identity_records WHERE source_system='pos';

-- COMMAND ----------
CREATE OR REPLACE VIEW {{catalog}}.{{source_schema}}.tokenized_payment_profiles AS
SELECT * FROM {{catalog}}.{{source_schema}}.source_identity_records WHERE source_system='payments';

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.{{truth_schema}}.generator_manifest
USING DELTA COMMENT 'RESTRICTED generator version, seed and expected row counts' AS
SELECT '{{generator_version}}' generator_version,'{{seed}}' generator_seed,current_timestamp() generated_at,
  {{people}} expected_people,
  (SELECT sum(record_count) FROM {{catalog}}.{{truth_schema}}.guest_relationships) expected_identity_records,{{pos_rows}} expected_pos_transactions,
  120000 expected_sevenrooms_reservations,200000 expected_meandu_orders,80000 expected_moshtix_tickets,
  30000 expected_momentus_bookings,400000 expected_payment_events;

-- COMMAND ----------
-- Record volume follows the drawn relationship depth: about 2.45 records per guest on average.
SELECT assert_true(count(*)=(SELECT sum(record_count) FROM {{catalog}}.{{truth_schema}}.guest_relationships)
    AND count(*) BETWEEN 2.2*{{people}} AND 2.7*{{people}},
  'source_identity_records row count differs from the drawn relationship depth')
FROM {{catalog}}.{{source_schema}}.source_identity_records;

-- COMMAND ----------
-- Previous contact details are plausible history, never another guest's current identifier.
WITH collisions AS (
  SELECT count(*) n FROM {{catalog}}.{{truth_schema}}.guest_relationships g
  JOIN {{catalog}}.{{truth_schema}}.canonical_people c ON c.personal_email=g.previous_personal_email
  UNION ALL
  SELECT count(*) FROM {{catalog}}.{{truth_schema}}.guest_relationships g
  JOIN {{catalog}}.{{truth_schema}}.canonical_people c ON c.personal_phone=g.previous_personal_phone
)
SELECT assert_true(
  (SELECT sum(n) FROM collisions)=0
    AND count(previous_personal_email)=count(DISTINCT previous_personal_email)
    AND count(previous_personal_phone)=count(DISTINCT previous_personal_phone),
  'previous contact details collide with another guest')
FROM {{catalog}}.{{truth_schema}}.guest_relationships;

-- COMMAND ----------
SELECT assert_true(count(*)=count(DISTINCT source_record_id),'source_record_id must be unique')
FROM {{catalog}}.{{source_schema}}.source_identity_records;

-- COMMAND ----------
SELECT assert_true(count(*)={{people}},'canonical_people row count differs from contract')
FROM {{catalog}}.{{truth_schema}}.canonical_people;

-- COMMAND ----------
SELECT assert_true(count(*)={{people}} AND count(*)=count(DISTINCT person_index),
  'every canonical person must have exactly one household membership')
FROM {{catalog}}.{{truth_schema}}.household_membership;

-- COMMAND ----------
WITH distribution AS (
  SELECT count(*) households,
    count_if(household_size=1) solo_households,
    count_if(household_size>1) shared_households,
    min(household_size) min_size,max(household_size) max_size
  FROM {{catalog}}.{{truth_schema}}.households
)
SELECT assert_true(
  min_size=1 AND max_size<=5
    AND solo_households*1.0/households BETWEEN 0.28 AND 0.38
    AND shared_households>0,
  'household sizes or solo-household distribution differ from contract')
FROM distribution;

-- COMMAND ----------
WITH shared AS (
  SELECT truth_household_id,count(*) people,count(DISTINCT family_name) surnames
  FROM {{catalog}}.{{truth_schema}}.canonical_people
  GROUP BY truth_household_id
  HAVING count(*)>1
), distribution AS (
  SELECT count(*) households,
    count_if(surnames=1) all_same,
    count_if(surnames>1 AND surnames<people) partly_shared,
    count_if(surnames=people) all_different
  FROM shared
)
SELECT assert_true(
  all_same*1.0/households BETWEEN 0.38 AND 0.58
    AND partly_shared*1.0/households BETWEEN 0.10 AND 0.28
    AND all_different*1.0/households BETWEEN 0.25 AND 0.48,
  'shared-household surname distribution differs from contract')
FROM distribution;

-- COMMAND ----------
SELECT assert_true(
  count(*)=count(DISTINCT personal_email)
    AND count(work_email)=count(DISTINCT work_email)
    AND count_if(personal_email_origin='NAME_BASED'
      AND instr(split(personal_email,'@')[0],regexp_replace(lower(family_name),'[^a-z]',''))=0
      AND instr(split(personal_email,'@')[0],regexp_replace(lower(given_name),'[^a-z]',''))=0)=0
    AND count_if(personal_email_origin='INITIALS'
      AND NOT startswith(personal_email,lower(concat(substr(regexp_replace(given_name,'[^A-Za-z]',''),1,1),substr(regexp_replace(family_name,'[^A-Za-z]',''),1,1)))))=0
    AND count_if(personal_email_origin='OPAQUE_LEGACY'
      AND (startswith(personal_email,regexp_replace(lower(family_name),'[^a-z]',''))
        OR startswith(personal_email,regexp_replace(lower(given_name),'[^a-z]',''))))=0,
  'canonical email uniqueness, coherence or provenance differs from contract')
FROM {{catalog}}.{{truth_schema}}.canonical_people;

-- COMMAND ----------
SELECT assert_true(count(*)=0,
  'common-name challenges must be coherent canonical identities, not renamed records')
FROM {{catalog}}.{{truth_schema}}.canonical_people
WHERE person_index BETWEEN 2600 AND 2999
  AND personal_email_origin='NAME_BASED'
  AND instr(split(personal_email,'@')[0],regexp_replace(lower(family_name),'[^a-z]',''))=0
  AND instr(split(personal_email,'@')[0],regexp_replace(lower(given_name),'[^a-z]',''))=0;

-- COMMAND ----------
SELECT assert_true(count(*)=7 AND count(DISTINCT scenario_key)=7,
  'curated scenario contract must contain exactly seven unique scenarios')
FROM {{catalog}}.{{truth_schema}}.curated_scenario_cases;

-- COMMAND ----------
WITH truth_check AS (
  SELECT c.scenario_key,c.expected_same_person,
    (l.truth_person_id=r.truth_person_id) actual_same_person
  FROM {{catalog}}.{{truth_schema}}.curated_scenario_cases c
  JOIN {{catalog}}.{{truth_schema}}.identity_record_truth l
    ON l.identity_record_id=c.left_identity_record_id
  JOIN {{catalog}}.{{truth_schema}}.identity_record_truth r
    ON r.identity_record_id=c.right_identity_record_id
)
SELECT assert_true(count(*)=7 AND count_if(expected_same_person<>actual_same_person)=0,
  'curated scenario pair truth is incomplete or inconsistent')
FROM truth_check;

-- COMMAND ----------
WITH source_with_id AS (
  SELECT *,sha2(concat_ws('|',lower(trim(source_system)),trim(source_record_id)),256) identity_record_id
  FROM {{catalog}}.{{source_schema}}.source_identity_records
), pair_data AS (
  SELECT c.*,l.source_system left_source,l.source_record_id left_source_record_id,
    l.given_name left_given,l.family_name left_family,l.full_name left_full_name,
    l.personal_email left_personal_email,l.work_email left_work_email,
    l.personal_phone left_personal_phone,l.work_phone left_work_phone,l.date_of_birth left_dob,
    l.home_address_line1 left_home_address,l.work_address_line1 left_work_address,
    r.source_system right_source,r.source_record_id right_source_record_id,
    r.given_name right_given,r.family_name right_family,r.full_name right_full_name,
    r.personal_email right_personal_email,r.work_email right_work_email,
    r.personal_phone right_personal_phone,r.work_phone right_work_phone,r.date_of_birth right_dob,
    r.home_address_line1 right_home_address,r.work_address_line1 right_work_address
  FROM {{catalog}}.{{truth_schema}}.curated_scenario_cases c
  JOIN source_with_id l ON l.identity_record_id=c.left_identity_record_id
  JOIN source_with_id r ON r.identity_record_id=c.right_identity_record_id
), scenario_checks AS (
  SELECT scenario_key,CASE scenario_key
    WHEN 'hangul_english' THEN left_full_name='김민준' AND right_full_name='Minjun Kim'
      AND left_home_address='서울특별시 강남구 테헤란로 152' AND right_home_address='152 Teheran-ro'
    WHEN 'corporate_shared_contact' THEN left_work_email=right_work_email
      AND left_work_phone=right_work_phone AND left_work_address=right_work_address
      AND left_personal_email<>right_personal_email AND left_personal_phone<>right_personal_phone
      AND left_dob<>right_dob
    WHEN 'household_parent_child' THEN left_family=right_family
      AND substr(left_given,1,1)=substr(right_given,1,1)
      AND left_personal_phone=right_personal_phone AND left_home_address=right_home_address
      AND left_dob<>right_dob
    WHEN 'default_dob' THEN left_dob=DATE '1989-11-23' AND right_dob=DATE '1957-05-01'
    WHEN 'typo_sparse_booking' THEN right_source='sevenrooms'
      AND right_given='Mtailda' AND right_family='Wlison' AND right_home_address='7 Oxfrd Rd'
      AND right_personal_email IS NULL AND right_work_email IS NULL
      AND right_personal_phone IS NULL AND right_work_phone IS NULL AND right_dob IS NULL
    WHEN 'within_source_duplicate' THEN left_source='sevenrooms' AND right_source='sevenrooms'
      AND left_source_record_id<>right_source_record_id AND left_personal_email=right_personal_email
      AND left_personal_phone=right_personal_phone AND left_dob=right_dob
    WHEN 'english_chinese_name' THEN left_given='Emily' AND right_given='Yutong'
      AND left_family='Zhang' AND right_family='Zhang' AND right_full_name='张雨桐'
      AND left_dob=right_dob AND left_dob<>DATE '1957-05-01'
      AND left_personal_phone=right_personal_phone AND left_home_address<>right_home_address
    ELSE false END scenario_valid
  FROM pair_data
)
SELECT assert_true(count(*)=7 AND count_if(NOT coalesce(scenario_valid,false))=0,
  'one or more curated scenario evidence paths are invalid')
FROM scenario_checks;

-- COMMAND ----------
SELECT assert_true(
  count_if(date_of_birth=DATE '1957-05-01')>=25
    AND count_if(date_of_birth=DATE '1957-05-01')*1.0/count(*)>=0.005,
  'the Momentus default DOB must be frequent enough for source-level detection')
FROM {{catalog}}.{{source_schema}}.source_identity_records
WHERE source_system='momentus';

-- COMMAND ----------
SELECT assert_true(count(*)={{pos_rows}},'pos_transactions row count differs from contract')
FROM {{catalog}}.{{source_schema}}.pos_transactions;

-- COMMAND ----------
WITH actual AS (
  SELECT 'sevenrooms' dataset,count(*) rows FROM {{catalog}}.{{source_schema}}.sevenrooms_reservations UNION ALL
  SELECT 'me_and_u',count(*) FROM {{catalog}}.{{source_schema}}.me_and_u_orders UNION ALL
  SELECT 'moshtix',count(*) FROM {{catalog}}.{{source_schema}}.moshtix_tickets UNION ALL
  SELECT 'momentus',count(*) FROM {{catalog}}.{{source_schema}}.momentus_bookings UNION ALL
  SELECT 'payments',count(*) FROM {{catalog}}.{{source_schema}}.tokenized_payment_events
), expected(dataset,rows) AS (VALUES ('sevenrooms',120000L),('me_and_u',200000L),('moshtix',80000L),('momentus',30000L),('payments',400000L))
SELECT assert_true(count_if(a.rows<>e.rows)=0,'activity row count differs from contract')
FROM actual a JOIN expected e USING (dataset);

-- COMMAND ----------
SELECT assert_true(count(*)=0,'visible source contract leaked a truth-bearing column')
FROM {{catalog}}.information_schema.columns
WHERE table_schema='{{source_schema}}' AND table_name='source_identity_records'
  AND lower(column_name) IN ('truth_person_id','truth_household_id','person_index','scenario_type','evaluation_split','generator_seed',
    'household_archetype','household_role','personal_email_origin','canonical_personal_email_origin');

-- COMMAND ----------
WITH activity_ids AS (
  SELECT source_record_id FROM {{catalog}}.{{source_schema}}.sevenrooms_reservations UNION ALL
  SELECT source_record_id FROM {{catalog}}.{{source_schema}}.me_and_u_orders UNION ALL
  SELECT source_record_id FROM {{catalog}}.{{source_schema}}.moshtix_tickets UNION ALL
  SELECT source_record_id FROM {{catalog}}.{{source_schema}}.momentus_bookings UNION ALL
  SELECT source_record_id FROM {{catalog}}.{{source_schema}}.pos_transactions UNION ALL
  SELECT source_record_id FROM {{catalog}}.{{source_schema}}.tokenized_payment_events
)
SELECT assert_true(count(*)=0,'activity contains an orphan source_record_id')
FROM activity_ids a
LEFT ANTI JOIN {{catalog}}.{{source_schema}}.source_identity_records i USING (source_record_id);
