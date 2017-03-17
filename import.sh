#!/bin/bash

##########
# Config #
##########

SCRIPT_DIR=$( cd "$( dirname "$0" )" && pwd )

# HOT TM2
# http://tasks.hotosm.org/?direction=desc&sort_by=created
MAX_PROJECT=2631

# Database
DB_NAME=hotosm_history_20170306
DB_CMD="psql --set ON_ERROR_STOP=1 -U osm -h localhost ${DB_NAME}"
${DB_CMD} -c "SELECT 1" || exit 1

# Scripts and programs
SCRAPER_APP_DIR=~/osm/src/hot-tm2-scraper
HISTORY_PARSER_DIR=~/osm/src/osm-history-parser/build
CHANGESETS_PARSER_DIR=~/osm/src/osm-changeset-parser
PYTHON=~/osm/ipython-env/env/bin/python
WGET_CMD="wget --progress=dot:giga"

# Data sources
HISTORY_DUMP_URL=http://planet.osm.org/pbf/full-history/history-latest.osm.pbf
# HISTORY_DUMP_URL=http://planet.osm.org/planet/full-history/history-latest.osm.bz2
CHANGESETS_DUMP_URL=http://planet.osm.org/planet/changesets-latest.osm.bz2

# Local directories for work files
ETL_DIR=~/osm/data/etl/${DB_NAME}
mkdir -p ${ETL_DIR} || exit 1

SCRAPER_DATA_DIR=${ETL_DIR}/hotosm-tm2

HISTORY_DUMP_DIR=${ETL_DIR}/osm-history
mkdir -p ${HISTORY_DUMP_DIR} || exit 1
HISTORY_DUMP_FILE=${HISTORY_DUMP_DIR}/`basename ${HISTORY_DUMP_URL}`

CHANGESETS_DUMP_DIR=${ETL_DIR}/osm-changesets
mkdir -p ${CHANGESETS_DUMP_DIR} || exit 1
CHANGESETS_DUMP_FILE_COMPRESSED=${CHANGESETS_DUMP_DIR}/`basename ${CHANGESETS_DUMP_URL}`
CHANGESETS_DUMP_FILE=${CHANGESETS_DUMP_DIR}/`basename ${CHANGESETS_DUMP_URL} .bz2`


#########
# Tools #
#########

function easy() {
    ionice -c2 -n7 nice -n 19 $@
}


###########
# Scraper #
###########


function do_scrape() {
  time ${SCRAPER_APP_DIR}/scraper/scrape.sh ${SCRAPER_DATA_DIR}/data 1 ${MAX_PROJECT} || return 1
  
  ${PYTHON} ${SCRAPER_APP_DIR}/parser/project_description.py \
    ${SCRAPER_DATA_DIR}/data \
    ${SCRAPER_DATA_DIR}/profiles || return 1
  
  # wc -l ${SCRAPER_DATA_DIR}/profiles/project_description.txt
  
  ${PYTHON} ${SCRAPER_APP_DIR}/parser/project_activity_period.py \
    ${SCRAPER_DATA_DIR}/data \
    ${SCRAPER_DATA_DIR}/profiles || return 1
  
  ${PYTHON} ${SCRAPER_APP_DIR}/parser/project_contributors.py \
    ${SCRAPER_DATA_DIR}/data \
    ${SCRAPER_DATA_DIR}/profiles || return 1
  
  ${PYTHON} \
    ${SCRAPER_APP_DIR}/parser/project_regions.py \
    ${SCRAPER_DATA_DIR}/data \
    ${SCRAPER_DATA_DIR}/shapefiles \
    hot_project_regions || return 1
  
  shp2pgsql -c -I ${SCRAPER_DATA_DIR}/shapefiles/hot_project_regions.shp hot_project_region > ${SCRAPER_DATA_DIR}/shapefiles/hot_project_region.sql || return 1
  
  ${DB_CMD} < ${SCRAPER_DATA_DIR}/shapefiles/hot_project_region.sql || return 1
  ${DB_CMD} -c "ALTER TABLE hot_project_region ADD COLUMN hot_project INTEGER; UPDATE hot_project_region SET hot_project=id::integer" || return 1
  
  # Optionally:
  ${PYTHON} ${SCRAPER_APP_DIR}/parser/project_tasks.py \
    ${SCRAPER_DATA_DIR}/data \
    ${SCRAPER_DATA_DIR}/profiles || return 1
  
  ${PYTHON} ${SCRAPER_APP_DIR}/parser/project_user_count.py \
    ${SCRAPER_DATA_DIR}/data \
    ${SCRAPER_DATA_DIR}/profiles || return 1
}


#############
# DB schema #
#############

function do_schema() {
  ${DB_CMD} < ${SCRIPT_DIR}/schema.sql || return 1
}


###############
# OSM history #
###############

function do_wget_history() {
  pushd ${HISTORY_DUMP_DIR}
  ${WGET_CMD} ${HISTORY_DUMP_URL} || return 1
  popd
  
  pushd ${CHANGESETS_DUMP_DIR}
  ${WGET_CMD} ${CHANGESETS_DUMP_URL} || return 1
  bzip2 -d ${CHANGESETS_DUMP_FILE_COMPRESSED} || return 1
  popd
}

#######################
# HOT username lookup #
#######################

function do_username_lookup() {

  # PART I -- users registered in TM2 projects
  
  time easy ${HISTORY_PARSER_DIR}/user-uid-name-map \
    ${HISTORY_DUMP_DIR}/history-latest.osm.pbf \
    ${ETL_DIR}/user_uid_name.txt || return 1
  # wc -l ${ETL_DIR}/user_uid_name.txt
  
  # Extract registered task contributors from HOT pages
  ${DB_CMD} -c "\copy hot_project_registered_contributor_name FROM '${SCRAPER_DATA_DIR}/profiles/project_contributors.txt' NULL AS '' csv delimiter '	' header" || return 1
  ${DB_CMD} -c "\copy uid_username FROM '${ETL_DIR}/user_uid_name.txt' NULL AS '' delimiter '	'" || return 1
  ${DB_CMD} -c "INSERT INTO hot_project_registered_contributor
    SELECT hn.hot_project, uu.uid, hn.username, num_tasks
    FROM hot_project_registered_contributor_name hn
    LEFT OUTER JOIN uid_username uu ON hn.username=uu.username;" || return 1
  # select (uid is null) v, count(distinct username) from hot_project_registered_contributor group by v;
  
  # PART II -- users who submitted tagged HOT changesets
  
  # Extract contributors from changeset history
  time easy ${PYTHON} ${CHANGESETS_PARSER_DIR}/hot_users.py \
    ${CHANGESETS_DUMP_FILE} \
    ${ETL_DIR}/changeset_hot_users.tsv || return 1
  
  ${DB_CMD} -c "\copy hot_project_history_contributor FROM '${ETL_DIR}/changeset_hot_users.tsv' NULL AS '' csv delimiter '	' header" || return 1
  
  # PART III -- combine these lists
  
  ${DB_CMD} -c "INSERT INTO hot_project_contributor
    SELECT 
      COALESCE(hp.hot_project, hc.hot_project), 
      COALESCE(hp.uid, hc.uid), 
      COALESCE(hp.username, hc.username),
      COALESCE(hp.has_submitted_tasks, false)
    FROM (
      SELECT *, true as has_submitted_tasks
      FROM hot_project_registered_contributor
      WHERE uid IS NOT NULL
    ) hp
    FULL OUTER JOIN (
      SELECT *, NULL as has_submitted_tasks -- unknown
      FROM hot_project_history_contributor
    ) hc ON (hp.hot_project=hc.hot_project AND hp.uid=hc.uid)
    GROUP BY 
      COALESCE(hp.hot_project, hc.hot_project), 
      COALESCE(hp.uid, hc.uid), 
      COALESCE(hp.username, hc.username),
      COALESCE(hp.has_submitted_tasks, false);" || return 1
  
  # Extract userlist
  
  ${DB_CMD} -c "\copy (SELECT DISTINCT uid FROM hot_project_contributor WHERE uid IS NOT NULL) TO '${ETL_DIR}/hot-userids.txt' CSV" || return 1
}

##########################
# Other HOT stats tables #
##########################

function do_info_tables() {

  ${DB_CMD} -c "\copy hot_project_description FROM '${SCRAPER_DATA_DIR}/profiles/project_description.txt' csv delimiter '	' header" || return 1
  ${DB_CMD} -c "\copy hot_project_activity FROM '${SCRAPER_DATA_DIR}/profiles/project_activity.txt' NULL AS '' csv delimiter '	' header" || return 1
  ${DB_CMD} -c "INSERT INTO hot_project_changeset_tag SELECT * FROM etl_view_hot_project_changeset_tag;" || return 1

  # Optionally
  # ${DB_CMD} -c "\copy hot_project_tasks FROM '${SCRAPER_DATA_DIR}/profiles/project_tasks.txt' csv delimiter '	' header" || exit 1
}

###################
# Extract history #
###################

function do_extract_history() {
  time easy ${HISTORY_PARSER_DIR}/user-edit-history \
    ${HISTORY_DUMP_DIR}/history-latest.osm.pbf \
    ${ETL_DIR}/hot-userids.txt \
    ${ETL_DIR}/node_edits.txt \
    ${ETL_DIR}/way_edits.txt \
    ${ETL_DIR}/rel_edits.txt || return 1
  
  time easy ${HISTORY_PARSER_DIR}/user-tag-edit-history \
    ${HISTORY_DUMP_DIR}/history-latest.osm.pbf \
    ${ETL_DIR}/hot-userids.txt \
    ${ETL_DIR}/node_tag_edits.txt \
    ${ETL_DIR}/way_tag_edits.txt \
    ${ETL_DIR}/rel_tag_edits.txt || return 1
  
  ls -lh ${ETL_DIR}
}

################
# Load history #
################

function do_load_history() {
  time pv ${ETL_DIR}/node_edits.txt | ${DB_CMD} -c "COPY node_edits FROM STDIN NULL AS ''" || return 1
  time ${DB_CMD} -c "VACUUM ANALYZE node_edits" || return 1
  # select count(*) from node_edits;
  
  time pv ${ETL_DIR}/way_edits.txt | ${DB_CMD} -c "COPY way_edits FROM STDIN NULL AS ''" || return 1
  time ${DB_CMD} -c "VACUUM ANALYZE way_edits" || return 1
  # select count(*) from way_edits;
  
  time pv ${ETL_DIR}/rel_edits.txt | ${DB_CMD} -c "COPY rel_edits FROM STDIN NULL AS ''" || return 1
  time ${DB_CMD} -c "VACUUM ANALYZE rel_edits" || return 1
  # select count(*) from rel_edits;
  
  time pv ${ETL_DIR}/node_tag_edits.txt | ${DB_CMD} -c "COPY node_tag_edits FROM STDIN NULL AS ''" || return 1
  time ${DB_CMD} -c "VACUUM ANALYZE node_tag_edits" || return 1
  # select count(*) from node_tag_edits;
  
  time pv ${ETL_DIR}/way_tag_edits.txt | ${DB_CMD} -c "COPY way_tag_edits FROM STDIN NULL AS ''" || return 1
  time ${DB_CMD} -c "VACUUM ANALYZE way_tag_edits" || return 1
  # select count(*) from way_tag_edits;
  
  time pv ${ETL_DIR}/rel_tag_edits.txt | ${DB_CMD} -c "COPY rel_tag_edits FROM STDIN NULL AS ''" || return 1
  time ${DB_CMD} -c "VACUUM ANALYZE rel_tag_edits" || return 1
  # select count(*) from rel_tag_edits;
}

function do_clear_history_files() {
  # TODO: compress the raw data files instead?
    rm ${ETL_DIR}/node_edits.txt
    rm ${ETL_DIR}/way_edits.txt
    rm ${ETL_DIR}/rel_edits.txt
    rm ${ETL_DIR}/node_tag_edits.txt
    rm ${ETL_DIR}/way_tag_edits.txt
    rm ${ETL_DIR}/rel_tag_edits.txt
}

####################
# Deletion history #
####################

function do_deletion_history() {
  # Extract object list
  
  ${DB_CMD} -c "\copy (SELECT DISTINCT id FROM way_edits) TO '${ETL_DIR}/way-ids.txt' CSV" || return 1
  
  time easy ${HISTORY_PARSER_DIR}/way-deletion-history \
    ${HISTORY_DUMP_DIR}/history-latest.osm.pbf \
    ${ETL_DIR}/way-ids.txt \
    ${ETL_DIR}/way_deletions.txt || return 1
  
  time pv ${ETL_DIR}/way_deletions.txt | ${DB_CMD} -c "COPY way_deletions FROM STDIN NULL AS ''" || return 1
  time ${DB_CMD} -c "VACUUM ANALYZE way_deletions" || return 1
  # select count(*) from way_deletions;
}

###################
# PostGIS geojoin #
###################

function do_geojoin() {
  # Changeset aggregation
  
  time ${DB_CMD} -c 'SET temp_buffers = "200MB"; 
  INSERT INTO changeset SELECT * FROM etl_view_changeset;
  SET temp_buffers = "8MB";' || return 1
  
  # The final join
  
  time ${DB_CMD} -c 'SET temp_buffers = "200MB";
  INSERT INTO changeset_hot_project SELECT * FROM etl_view_changeset_hot_project;
  SET temp_buffers = "8MB";' || return 1
}

##################
# Changeset tags #
##################

function do_changeset_tags() {
  time easy ${PYTHON} ${CHANGESETS_PARSER_DIR}/changeset_tags.py \
    ${CHANGESETS_DUMP_FILE} \
    ${ETL_DIR}/changeset_meta.tsv \
    ${ETL_DIR}/changeset_meta_tags.tsv || return 1
  
  time ${DB_CMD} -c "\copy changeset_meta FROM '${ETL_DIR}/changeset_meta.tsv' NULL AS '' csv delimiter '	' header" || return 1
  time ${DB_CMD} -c "\copy changeset_meta_tags FROM '${ETL_DIR}/changeset_meta_tags.tsv' NULL AS '' csv delimiter '	' header" || return 1
  
  # Editor use per changeset
  ${DB_CMD} -c "INSERT INTO changeset_editor SELECT * FROM etl_view_changeset_editor;" || return 1
  
  # Changeset comment tags
  ${DB_CMD} -c "INSERT INTO changeset_comment SELECT * FROM etl_view_changeset_comment;" || return 1
}

#################
# Edit sessions #
#################

function do_edit_sessions() {
  ${DB_CMD} -c "INSERT INTO user_hmp_session_changeset SELECT * FROM etl_view_user_hmp_session_changeset" || return 1
  ${DB_CMD} -c "DROP TABLE IF EXISTS user_hmp_session; CREATE TABLE user_hmp_session AS SELECT * FROM etl_view_user_hmp_session;" || return 1
  ${DB_CMD} -c "CREATE UNIQUE INDEX idx_user_hmp_session_uid_hot_project_session ON user_hmp_session(uid, hot_project, session);" || return 1
}

##########################
# First HOT contribution #
##########################

function do_first_contribution() {
  ${DB_CMD} -c "DROP TABLE IF EXISTS user_first_hot_contribution;
  CREATE TABLE user_first_hot_contribution AS
    SELECT DISTINCT ON (uid) uid, hot_project, first_date date,
      first_date - INTERVAL '12h' start_of_day,
      (first_date)::time tz_offset
    FROM user_hmp_session 
    ORDER BY uid, first_date ASC;" || return 1
  
  ${DB_CMD} -c "DROP TABLE IF EXISTS user_prior_experience;
  CREATE TABLE user_prior_experience AS
    SELECT u.uid,
      count(distinct TO_CHAR(c.first_date - u.tz_offset, 'YYYY-MM-DD')) num_days_with_edits
    FROM user_first_hot_contribution u
    LEFT OUTER JOIN changeset c ON (c.uid=u.uid AND c.last_date<u.start_of_day)
    GROUP BY u.uid;" || return 1
  
  ${DB_CMD} -c "DROP TABLE IF EXISTS user_hot_contributions;
  CREATE TABLE user_hot_contributions AS
    SELECT u.uid, 
      min(first_date) first_date, max(last_date) last_date,
      count(distinct TO_CHAR(s.first_date - u.tz_offset, 'YYYY-MM-DD')) num_days_with_edits,
      count(distinct s.hot_project) num_projects, 
      sum(labour_hours) labour_hours,
      sum(num_edits) num_edits
    FROM user_first_hot_contribution u
    JOIN user_hmp_session s ON (s.uid=u.uid)
    GROUP BY u.uid;" || return 1
}

###############
# Basic stats #
###############

function do_show_stats() {
  # Table sizes
  
  ${DB_CMD} -c "SELECT n.nspname as schema, relname, pg_catalog.pg_size_pretty(pg_total_relation_size(pg_class.oid::regclass)) as size
    FROM pg_class
    JOIN pg_catalog.pg_namespace n ON n.oid=pg_class.relnamespace
    WHERE pg_class.relkind = 'r'::char
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY relpages DESC;" || return 1
  
  # Timeline of activity
  
  ${DB_CMD} -c "SELECT to_char(last_date, 'YYYY-MM') as month, 
    count(distinct hot_project) num_projects, 
    count(distinct uid) num_users, 
    sum(num_edits) num_edits
  FROM changeset c
  JOIN changeset_hot_project ch ON (c.changeset=ch.changeset) 
  GROUP BY month;" || return 1
  
  # Most active projects
  
  ${DB_CMD} -c "select 
    d.hot_project, substring(title for 50) title, 
    count(distinct uid) num_users, 
    sum(num_edits) num_edits, 
    count(*) num_changesets, 
    CASE is_private WHEN true THEN 't' ELSE '' END priv, 
    CASE is_archived WHEN true THEN 't' ELSE '' END arch, 
    CASE is_draft WHEN true THEN 't' ELSE '' END draft 
  from changeset c
  JOIN changeset_hot_project ch ON (c.changeset=ch.changeset) 
  join hot_project_description d on (ch.hot_project=d.hot_project) 
  group by d.hot_project, title, is_private, is_archived, is_draft
  order by num_users desc
  limit 100;" || return 1
 }

########
# Main #
########

do_scrape || exit 1
do_schema || exit 1
do_wget_history || exit 1
do_username_lookup || exit 1
do_info_tables || exit 1
do_extract_history || exit 1
do_load_history || exit 1
do_clear_history_files || exit 1
do_deletion_history || exit 1
do_geojoin || exit 1
do_changeset_tags || exit 1
do_edit_sessions || exit 1
do_first_contribution || exit 1

