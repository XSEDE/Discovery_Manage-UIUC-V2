#!/bin/bash

PG_DUMP=/opt/local/lib/postgresql96/bin/pg_dump
${PG_DUMP} -h portaldb-test.clv6gplzp1az.us-east-2.rds.amazonaws.com -U xsede_user \
    --no-acl \
    -t "*.resource"  -t "*.provider" -t "*.series" -t "*.keyword" -t "*.resources_keywords" \
    portalTest \
    > uiuc_resources.dump
