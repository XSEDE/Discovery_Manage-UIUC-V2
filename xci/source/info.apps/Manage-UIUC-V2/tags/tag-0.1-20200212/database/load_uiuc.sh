#!/bin/bash -x

DB="-h infodb.xsede.org"
DB="-h localhost"

psql ${DB} -U pixo_user uiuctest <<EOF
drop table resources_keywords;
drop table resource;
drop table provider;
drop table series;
drop table keyword;
EOF

psql ${DB} -U pixo_user uiuctest \
    < uiuc_resources.dump

psql ${DB} -U pixo_user uiuctest <<EOF
grant select on resources_keywords to xsede_user;
grant select on resource to xsede_user;
grant select on provider to xsede_user;
grant select on series to xsede_user;
grant select on keyword to xsede_user;
EOF

psql ${DB} -a -U pixo_user uiuctest <<EOF
select count(*) from resources_keywords;
select count(*) from resource;
select count(*) from provider;
select count(*) from series;
select count(*) from keyword;
EOF
