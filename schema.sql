--
-- Contributor IDs
--

DROP TABLE IF EXISTS hot_project_registered_contributor_name;
CREATE TABLE hot_project_registered_contributor_name (
  hot_project       INTEGER NOT NULL,
  username          TEXT NOT NULL,
  num_tasks         INTEGER NOT NULL
);

DROP TABLE IF EXISTS uid_username;
CREATE TABLE uid_username (
  uid         INTEGER NOT NULL,
  username    TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_uid_username_username ON uid_username(username);
CREATE UNIQUE INDEX idx_uid_username_uid ON uid_username(uid);

-- All users registered as participants in the tasking manager.
-- This excludes anyone who did not mark any projects as "done".
-- `uid` is NULL if the username was not found in the OSM edit history.
DROP TABLE IF EXISTS hot_project_registered_contributor;
CREATE TABLE hot_project_registered_contributor (
  hot_project       INTEGER NOT NULL,
  uid               INTEGER,
  username          TEXT NOT NULL,
  num_tasks         INTEGER NOT NULL
);

-- All users who submitted changesets linked to a HOT project.
DROP TABLE IF EXISTS hot_project_history_contributor;
CREATE TABLE hot_project_history_contributor (
  hot_tag           TEXT NOT NULL,
  hot_project       INTEGER NOT NULL,
  uid               INTEGER NOT NULL,
  username          TEXT NOT NULL
);

-- All users who EITHER submitted a task as "done" in the tasking manager, or
-- at least submitted one tagged changeset for the project.
DROP TABLE IF EXISTS hot_project_contributor;
CREATE TABLE hot_project_contributor (
  hot_project         INTEGER NOT NULL,
  uid                 INTEGER NOT NULL,
  username            TEXT NOT NULL,
  has_submitted_tasks BOOL NOT NULL
);

CREATE UNIQUE INDEX idx_hot_project_contributor_project_uid ON hot_project_contributor(hot_project, uid);

--
-- Other HOT stats tables
--

DROP TABLE IF EXISTS hot_project_description CASCADE;
CREATE TABLE hot_project_description (
  hot_project       INTEGER NOT NULL,
  title             TEXT,
  is_private        BOOLEAN NOT NULL,
  is_archived       BOOLEAN NOT NULL,
  is_draft          BOOLEAN NOT NULL,
  changeset_comment TEXT
);

CREATE UNIQUE INDEX idx_hot_project_description_hot_project ON hot_project_description(hot_project);

DROP TABLE IF EXISTS hot_project_activity CASCADE;
CREATE TABLE hot_project_activity (
  hot_project       INTEGER NOT NULL,
  first_date        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  last_date         TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  perc_done         NUMERIC,
  perc_validated    NUMERIC
);

CREATE UNIQUE INDEX idx_hot_project_activity_hot_project ON hot_project_activity(hot_project);

DROP TABLE IF EXISTS hot_project_changeset_tag CASCADE;
CREATE TABLE hot_project_changeset_tag (
  hot_project   INTEGER NOT NULL,
  hot_tag       TEXT NOT NULL
);

DROP VIEW IF EXISTS etl_view_hot_project_changeset_tag CASCADE;
CREATE VIEW etl_view_hot_project_changeset_tag AS
  SELECT 
    hot_project,
    substring(changeset_comment, '.*(#hotosm[^ /#,+;.:]+-[0-9]+).*') as hot_tag
  FROM hot_project_description
  WHERE changeset_comment ~ '.*#hotosm[^ /#,+;.:]+-[0-9]+.*';

-- Optionally

-- DROP TABLE IF EXISTS hot_project_tasks;
-- CREATE TABLE hot_project_tasks (
--   hot_project       INTEGER NOT NULL,
--   num_tasks         INTEGER NOT NULL
-- );
-- 
-- CREATE UNIQUE INDEX idx_hot_project_tasks_hot_project ON hot_project_tasks(hot_project);

-- 
-- History tables
--

DROP TABLE IF EXISTS node_edits CASCADE;
CREATE TABLE node_edits (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER,
  latitude    NUMERIC,
  longitude   NUMERIC
);

DROP TABLE IF EXISTS way_edits CASCADE;
CREATE TABLE way_edits (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER
);

DROP TABLE IF EXISTS rel_edits CASCADE;
CREATE TABLE rel_edits (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER
);

DROP TABLE IF EXISTS node_tag_edits CASCADE;
CREATE TABLE node_tag_edits (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER,
  key         TEXT,
  value       TEXT
);

DROP TABLE IF EXISTS way_tag_edits CASCADE;
CREATE TABLE way_tag_edits (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER,
  key         TEXT,
  value       TEXT
);

DROP TABLE IF EXISTS rel_tag_edits CASCADE;
CREATE TABLE rel_tag_edits (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER,
  key         TEXT,
  value       TEXT
);

DROP TABLE IF EXISTS node_deletions CASCADE;
CREATE TABLE node_deletions (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER
);

DROP TABLE IF EXISTS way_deletions CASCADE;
CREATE TABLE way_deletions (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER
);

DROP TABLE IF EXISTS rel_deletions CASCADE;
CREATE TABLE rel_deletions (
  id          BIGINT NOT NULL,
  version     INTEGER NOT NULL,
  changeset   INTEGER NOT NULL,
  timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid         INTEGER
);


-- 
-- PostGIS geojoin
--

CREATE OR REPLACE FUNCTION make_bbox(
  min_lat numeric, max_lat numeric,
  min_lon numeric, max_lon numeric) 
RETURNS geometry 
AS $$
  BEGIN
    RETURN 
      ST_MakePolygon(ST_MakeLine(array[
        ST_MakePoint(min_lon, min_lat),
        ST_MakePoint(max_lon, min_lat),
        ST_MakePoint(max_lon, max_lat),
        ST_MakePoint(min_lon, max_lat),
        ST_MakePoint(min_lon, min_lat)]));
  END;
$$ LANGUAGE plpgsql;

DROP VIEW IF EXISTS etl_view_changeset_summary_node_edits;
CREATE VIEW etl_view_changeset_summary_node_edits AS
  SELECT changeset, min(uid) uid, 
    min(timestamp) as first_date, max(timestamp) as last_date,
    min(latitude) as min_lat, max(latitude) as max_lat,
    min(longitude) as min_lon, max(longitude) as max_lon,
    count(*) as num_edits
  FROM node_edits
  GROUP BY changeset;

DROP VIEW IF EXISTS etl_view_changeset_summary_way_edits;
CREATE VIEW etl_view_changeset_summary_way_edits AS
  SELECT changeset, min(uid) uid, 
    min(timestamp) as first_date, max(timestamp) as last_date,
    count(*) as num_edits
  FROM way_edits
  GROUP BY changeset;

DROP VIEW IF EXISTS etl_view_changeset_summary_rel_edits;
CREATE VIEW etl_view_changeset_summary_rel_edits AS
  SELECT changeset, min(uid) uid, 
    min(timestamp) as first_date, max(timestamp) as last_date,
    count(*) as num_edits
  FROM rel_edits
  GROUP BY changeset;

---

DROP TABLE IF EXISTS changeset;
CREATE TABLE changeset (
  changeset   INTEGER NOT NULL,
  uid         INTEGER NOT NULL,
  first_date  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  last_date   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  min_lat     NUMERIC,
  max_lat     NUMERIC,
  min_lon     NUMERIC,
  max_lon     NUMERIC,
  bbox        GEOMETRY,
  node_edits  INTEGER NOT NULL,
  way_edits   INTEGER NOT NULL,
  rel_edits   INTEGER NOT NULL,
  num_edits   INTEGER NOT NULL
);

DROP VIEW IF EXISTS etl_view_changeset;
CREATE VIEW etl_view_changeset AS
  SELECT 
    COALESCE(n.changeset, w.changeset, r.changeset) as changeset,
    COALESCE(n.uid, w.uid, r.uid) as uid,
    LEAST(n.first_date, w.first_date, r.first_date) first_date,
    GREATEST(n.last_date, w.last_date, r.last_date) last_date, 
    min_lat, max_lat, min_lon, max_lon,
    CASE 
      WHEN min_lat IS NOT NULL THEN
        make_bbox(min_lat, max_lat, min_lon, max_lon) 
      ELSE NULL
    END bbox,
    COALESCE(n.num_edits, 0) node_edits,
    COALESCE(w.num_edits, 0) way_edits,
    COALESCE(r.num_edits, 0) rel_edits, 
    ( COALESCE(n.num_edits, 0) + 
      COALESCE(w.num_edits, 0) + 
      COALESCE(r.num_edits, 0)) num_edits
  FROM etl_view_changeset_summary_node_edits n
  FULL OUTER JOIN etl_view_changeset_summary_way_edits w ON (n.changeset=w.changeset)
  FULL OUTER JOIN etl_view_changeset_summary_rel_edits r ON (COALESCE(n.changeset, w.changeset)=r.changeset);

-- Tools

CREATE OR REPLACE FUNCTION bbox_geog_area(
  min_lat numeric, max_lat numeric, 
  min_lon numeric, max_lon numeric) RETURNS numeric 
AS $$
  DECLARE
    smallest_lat CONSTANT numeric := 0.0001;
  BEGIN
    IF min_lat >= max_lat THEN RETURN 0::numeric; END IF;
    IF min_lon >= max_lon THEN RETURN 0::numeric; END IF;
    -- crosses equator?
    IF (min_lat<=0 AND max_lat>=0) THEN
      RETURN 
        bbox_geog_area(min_lat, -smallest_lat, min_lon, max_lon) +
        bbox_geog_area(smallest_lat, max_lat, min_lon, max_lon);
    ELSE
      RETURN ST_Area(make_bbox(min_lat, max_lat, min_lon, max_lon)::geography)::numeric;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_world_bbox() 
RETURNS GEOMETRY 
AS $$
  BEGIN
    RETURN make_bbox(-90, 90, -180, 180);
  END;
$$ LANGUAGE plpgsql;

-- The final join

DROP TABLE IF EXISTS changeset_hot_project;
CREATE TABLE changeset_hot_project (
  changeset   INTEGER NOT NULL,
  hot_project INTEGER NOT NULL
);

DROP VIEW IF EXISTS etl_view_changeset_hot_project;
CREATE VIEW etl_view_changeset_hot_project AS
  SELECT c.changeset, r.hot_project
  FROM hot_project_region r
  JOIN hot_project_contributor p ON (p.hot_project=r.hot_project)
  JOIN hot_project_activity a ON (a.hot_project=r.hot_project)
  JOIN hot_project_description d ON (d.hot_project=r.hot_project)
  JOIN changeset c ON (
    ST_Contains(get_world_bbox(), geom) AND           -- project area is within world bounds
    (d.is_private=FALSE AND d.is_draft=FALSE)) AND        -- project is publicly visible (may be archived)
    c.min_lat IS NOT NULL AND                             -- changeset has known coordinates
    (bbox_geog_area(min_lat, max_lat, min_lon, max_lon) <= 100 * 1000 * 1000) AND
                                                          -- changeset fits into 100km^2 bbox
    ST_Intersects(r.geom, c.bbox) AND                 -- changeset bbox intersects with geographic bounds of project
    c.uid=p.uid AND                                       -- changeset author is a project contributor
    ( a.first_date<=c.last_date AND                       -- changeset timestamp within project activity period
      a.last_date>=c.first_date );

--
-- Changeset tags
--

DROP TABLE IF EXISTS changeset_meta;
CREATE TABLE changeset_meta (
  changeset       INTEGER PRIMARY KEY,
  created_at      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  closed_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  uid             INTEGER NOT NULL,
  username        TEXT NOT NULL,
  num_changes     INTEGER NOT NULL,
  comments_count  INTEGER NOT NULL
);

DROP TABLE IF EXISTS changeset_meta_tags;
CREATE TABLE changeset_meta_tags (
  changeset   INTEGER NOT NULL,
  key         TEXT NOT NULL,
  value       TEXT
);

CREATE INDEX idx_changeset_meta_tags_key ON changeset_meta_tags(key);

--
-- Derived from changeset tags
--

DROP TABLE IF EXISTS changeset_editor;
CREATE TABLE changeset_editor (
  changeset   INTEGER NOT NULL,
  editor      TEXT NOT NULL,
  editor_full TEXT
);

DROP VIEW IF EXISTS etl_view_changeset_editor CASCADE;
CREATE VIEW etl_view_changeset_editor AS
  SELECT c.changeset,
    coalesce(substring(value, '(iD|JOSM|Potlatch).*'), 'Other') as editor,
    value as editor_full
  FROM changeset c
  LEFT OUTER JOIN changeset_meta_tags t ON (c.changeset=t.changeset AND t.key='created_by');
-- coalesce(substring(value, '(iD|JOSM|Potlatch|Merkaartor|rosemary|Vespucci|OsmAnd|Go Map!!|Pushpin|wheelmap).*'), 'Other') as editor,

DROP TABLE IF EXISTS changeset_comment;
CREATE TABLE changeset_comment (
  changeset   INTEGER NOT NULL,
  comment     TEXT NOT NULL
);

-- CREATE INDEX idx_changeset_comment_lower_comment ON changeset_comment(lower(comment));

DROP VIEW IF EXISTS etl_view_changeset_comment CASCADE;
CREATE VIEW etl_view_changeset_comment AS
  SELECT changeset, value as comment
  FROM changeset_meta_tags
  WHERE key='comment' AND value IS NOT NULL AND value!='';

--
-- Edit sessions
--

DROP SEQUENCE IF EXISTS user_hmp_session_counter CASCADE;
CREATE SEQUENCE user_hmp_session_counter;

DROP VIEW IF EXISTS etl_view_user_hmp_session_timeouts;
CREATE VIEW etl_view_user_hmp_session_timeouts AS
  SELECT uid, hot_project, changeset, first_date, last_date, 
    CASE 
      WHEN time_since_prev_cs > interval '1 hour' THEN NULL
      ELSE time_since_prev_cs
    END time_since_prev_cs
  FROM (
    SELECT uid, hot_project, c.changeset, first_date, last_date, 
      first_date - lag(last_date)
        OVER (partition by (uid, hot_project) ORDER BY first_date ASC)
        as time_since_prev_cs
    FROM changeset c
    JOIN changeset_hot_project ch ON (c.changeset=ch.changeset) 
  ) t;

DROP VIEW IF EXISTS etl_view_user_hmp_session_changeset;
CREATE VIEW etl_view_user_hmp_session_changeset AS
  SELECT uid, hot_project, 
    CASE 
      WHEN time_since_prev_cs IS NULL THEN nextval('user_hmp_session_counter')
      ELSE currval('user_hmp_session_counter')
    END as session,
    changeset
  FROM etl_view_user_hmp_session_timeouts 
  ORDER BY uid, hot_project, first_date;

DROP TABLE IF EXISTS user_hmp_session_changeset CASCADE;
CREATE TABLE user_hmp_session_changeset (
    uid         INTEGER NOT NULL,
    hot_project INTEGER NOT NULL,
    session     BIGINT NOT NULL,
    changeset   INTEGER NOT NULL
);

DROP VIEW IF EXISTS etl_view_user_hmp_session_size;
CREATE VIEW etl_view_user_hmp_session_size AS
  SELECT s.uid, s.hot_project, session, 
    min(c.first_date) first_date,
    max(c.last_date) last_date,
    EXTRACT (epoch FROM (max(c.last_date) - min(c.first_date)))::numeric / 3600 labour_hours,
    sum(node_edits) node_edits,
    sum(way_edits) way_edits,
    sum(rel_edits) rel_edits,
    sum(num_edits) num_edits
  FROM user_hmp_session_changeset s
  JOIN changeset c ON (s.changeset=c.changeset)
  GROUP BY s.uid, s.hot_project, session;

DROP VIEW IF EXISTS etl_view_user_hmp_session;
CREATE VIEW etl_view_user_hmp_session AS 
  SELECT uid, hot_project, s.session, s.first_date, s.last_date, 
    labour_hours + avg_lhpe * first_cs.num_edits labour_hours,
    s.node_edits,
    s.way_edits,
    s.rel_edits,
    s.num_edits
  FROM etl_view_user_hmp_session_size s
  JOIN (
    SELECT DISTINCT ON (session) session, s.changeset, num_edits
    FROM user_hmp_session_changeset s
    JOIN changeset c ON (c.changeset=s.changeset)
    ORDER BY session, first_date
  ) first_cs ON (first_cs.session=s.session)
  CROSS JOIN (
    SELECT avg(labour_hours / num_edits) avg_lhpe
    FROM etl_view_user_hmp_session_size
    WHERE labour_hours>0
  ) t;

