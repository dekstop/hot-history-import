
Scripts to import HOT TM2 edit histories into a Postgres database.
By Martin Dittus (@dekstop) in 2014/2015.

DISCLAIMER

Please only run these after careful study of what they're doing.
No need to create more problems for overworked server admins.

--

SOFTWARE REQUIREMENTS

Misc ETL tools/scripts:
https://github.com/dekstop/hot-tm2-scraper
https://github.com/dekstop/osm-history-parser
https://github.com/dekstop/osm-changeset-parser

These require:
- Bash, curl.
- cmake, a C++ compiler, Boost.
- Osmium 2.x libraries -- which in turn requires OSMPBF, GDAL, and likely more.
- A Python 2.x environment with GDAL and lxml.
- PostgreSQL 9.4 with PostGIS 2.1 (approx).

It can take some time to set these up... prepare yourself for lots of version conflicts and badly documented release issues. Geo processing software is still a pain to use in 2015.

---

SYSTEM REQUIREMENTS

The bandwidth to download ~70GB in OSM history files.

Lots of disk space -- in late 2015 it takes about 150GB in temp files (can be deleted after import), and 80GB for the database (can be reduced after import, depending on your needs).

It then takes about a day for the full import. Key performance bottlenecks are disk scans/seeks, although CPU can be a bottleneck during the initial parsing stages.

---

CREATING A DATABASE

Run as privileged postgres user. 

$ DB=hotosm_history_20151126
$ createuser osm --pwprompt
$ createdb $DB
$ psql -d $DB -c "CREATE EXTENSION postgis;"
$ psql -d $DB -c "GRANT CREATE ON DATABASE \"${DB}\" TO osm;"

Edit import.sh to reflect the database name.

And then:
$ ./import.sh

The importer script assumes that the "osm" user can run psql commands from the shell without a password prompt. There are various ways of setting this up without leaving the database exposed. The simplest option is the use of a ~/.pgpass file.

---

TODOs

TODO: unescape unicode html entities in the scraper/parser, e.g. "Savai&#39;i Island"

TODO: start a makefile version

Makefile examples:
https://github.com/stamen/toner-carto/blob/master/Makefile
http://mojodna.net/2015/01/07/make-for-data-using-make.html
http://bitaesthetics.com/posts/make-for-data-scientists.html

Can we organise this as a collection of makefiles? 
Have one shared stub, then add project-specific modules?
