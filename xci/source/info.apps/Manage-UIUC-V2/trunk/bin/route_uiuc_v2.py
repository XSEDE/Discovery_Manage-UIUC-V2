#!/usr/bin/env python

# Load UIUC resources information from a source (database) to a destination (warehouse)
import os
import pwd
import re
import sys
import argparse
import logging
import logging.handlers
import signal
import datetime
from datetime import datetime, tzinfo, timedelta
from time import sleep
import pytz
Central = pytz.timezone("US/Central")
UTC_TZ = pytz.utc

try:
    import http.client as httplib
except ImportError:
    import httplib
import psycopg2
import json
import ssl
import shutil

import django
django.setup()
from django.db import DataError, IntegrityError
from django.utils.dateparse import parse_datetime
from resource_cat.models import *
from processing_status.process import ProcessingActivity

import pdb

class UTC(tzinfo):
    def utcoffset(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return 'UTC'
    def dst(self, dt):
        return timedelta(0)
utc = UTC()

class HandleLoad():
    def __init__(self):
        self.args = None
        self.config = {}
        self.src = {}
        self.dest = {}
        self.stats = {}
        for var in ['uri', 'scheme', 'path']: # Where <full> contains <type>:<obj>
            self.src[var] = None
            self.dest[var] = None
        
        self.Affiliation = 'uiuc.edu'
        # Field Maps "fm" local to global
        self.fm = {
            'record_status': {
                '4': 'planned',
                '3': 'pre-production',
                '2': 'decommissioned',
                '1': 'production',
            }}

        self.have_column = ['resource_id', 'info_resourceid',
                            'resource_descriptive_name', 'resource_description',
                            'project_affiliation', 'provider_level',
                            'resource_status', 'current_statuses', 'updated_at']

        default_source = 'postgresql://localhost:5432/uiucTest'

        parser = argparse.ArgumentParser(epilog='File SRC|DEST syntax: file:<file path and name')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Messages source {postgresql} (default=postgresql)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Message destination {analyze, or warehouse} (default=analyze)')
        parser.add_argument('--ignore_dates', action='store_true', \
                            help='Ignore dates and force full resource refresh')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./route_uiuc.conf', \
                            help='Configuration file default=./route_uiuc.conf')
        parser.add_argument('--verbose', action='store_true', \
                            help='Verbose output')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        if self.args.pdb:
            pdb.set_trace()

        # Load configuration file
        config_path = os.path.abspath(self.args.config)
        try:
            with open(config_path, 'r') as file:
                conf=file.read()
                file.close()
        except IOError as e:
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            print('Error "{}" parsing config={}'.format(e, config_path))
            sys.exit(1)

        # Initialize logging from arguments, or config file, or default to WARNING as last resort
        numeric_log = None
        if self.args.log is not None:
            numeric_log = getattr(logging, self.args.log.upper(), None)
        if numeric_log is None and 'LOG_LEVEL' in self.config:
            numeric_log = getattr(logging, self.config['LOG_LEVEL'].upper(), None)
        if numeric_log is None:
            numeric_log = getattr(logging, 'WARNING', None)
        if not isinstance(numeric_log, int):
            raise ValueError('Invalid log level: {}'.format(numeric_log))
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(numeric_log)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', \
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], when='W6', \
                                                                 backupCount=999, utc=True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Verify arguments and parse compound arguments
        if not getattr(self.args, 'src', None): # Tests for None and empty ''
            if 'SOURCE_URL' in self.config:
                self.args.src = self.config['SOURCE_URL']
        if not getattr(self.args, 'src', None): # Tests for None and empty ''
            self.args.src = default_source
        idx = self.args.src.find(':')
        if idx > 0:
            (self.src['scheme'], self.src['path']) = (self.args.src[0:idx], self.args.src[idx+1:])
        else:
            (self.src['scheme'], self.src['path']) = (self.args.src, None)
        if self.src['scheme'] not in ['file', 'http', 'https', 'postgresql']:
            self.logger.error('Source not {file, http, https}')
            sys.exit(1)
        if self.src['scheme'] in ['http', 'https', 'postgresql']:
            if self.src['path'][0:2] != '//':
                self.logger.error('Source URL not followed by "//"')
                sys.exit(1)
            self.src['path'] = self.src['path'][2:]
        if len(self.src['path']) < 1:
            self.logger.error('Source is missing a database name')
            sys.exit(1)
        self.src['uri'] = self.args.src

        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            if 'DESTINATION' in self.config:
                self.args.dest = self.config['DESTINATION']
        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            self.args.dest = 'analyze'
        idx = self.args.dest.find(':')
        if idx > 0:
            (self.dest['scheme'], self.dest['path']) = (self.args.dest[0:idx], self.args.dest[idx+1:])
        else:
            self.dest['scheme'] = self.args.dest
        if self.dest['scheme'] not in ['file', 'analyze', 'warehouse']:
            self.logger.error('Destination not {file, analyze, warehouse}')
            sys.exit(1)
        self.dest['uri'] = self.args.dest

        if self.src['scheme'] in ['file'] and self.dest['scheme'] in ['file']:
            self.logger.error('Source and Destination can not both be a {file}')
            sys.exit(1)

    def Connect_Source(self, url):
        idx = url.find(':')
        if idx <= 0:
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)
            
        (type, obj) = (url[0:idx], url[idx+1:])
        if type not in ['postgresql']:
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)

        if obj[0:2] != '//':
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)
            
        obj = obj[2:]
        idx = obj.find('/')
        if idx <= 0:
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)
        (host, path) = (obj[0:idx], obj[idx+1:])
        idx = host.find(':')
        if idx > 0:
            port = host[idx+1:]
            host = host[:idx]
        elif type == 'postgresql':
            port = '5432'
        else:
            port = '5432'
        
        #Define our connection string
        conn_string = "host='{}' port='{}' dbname='{}' user='{}' password='{}'".format(host, port, path, self.config['SOURCE_DBUSER'], self.config['SOURCE_DBPASS'] )

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        self.logger.info('Connected to PostgreSQL database {} as {}'.format(path, self.config['SOURCE_DBUSER']))
        return(cursor)
 
    def Disconnect_Source(self, cursor):
        cursor.close()

    def Retrieve_Resources(self, cursor):
        try:
            sql = 'SELECT * from resource'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            if rowdict.get('record_status', None) not in [1, 2]:
                continue
            if 'last_updated' in rowdict and isinstance(rowdict['last_updated'], datetime):
                rowdict['last_updated'] = UTC_TZ.localize(rowdict['last_updated'])
            if 'start_date_time' in rowdict and isinstance(rowdict['start_date_time'], datetime):
                rowdict['start_date_time'] = UTC_TZ.localize(rowdict['start_date_time'])
            if 'end_date_time' in rowdict and isinstance(rowdict['end_date_time'], datetime):
                rowdict['end_date_time'] = UTC_TZ.localize(rowdict['end_date_time'])
            DATA[str(rowdict['id'])] = rowdict
        return(DATA)

    def Retrieve_Providers(self, cursor):
        try:
            sql = 'SELECT * from provider'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            DATA[str(rowdict['id'])] = rowdict
        return(DATA)

    def Retrieve_Resource_Tags(self, cursor):
        try:
            sql = 'SELECT * from tag'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)

        COLS = [desc.name for desc in cursor.description]
        tags = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            tags[rowdict['id']] = rowdict['label']
        
        try:
            sql = 'SELECT * from resources_tags'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)

        COLS = [desc.name for desc in cursor.description]
        resource_tags = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            id = str(rowdict['resource_id'])
            if id not in resource_tags:
                resource_tags[id] = []
            try:
                resource_tags[id].append(tags[rowdict['tag_id']])
            except:
                pass

        return(resource_tags)

    def Retrieve_Resource_Associations(self, cursor):
        try:
            sql = 'SELECT * from associated_resources'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            ID = str(rowdict['resource_id'])
            if ID not in DATA:
                DATA[ID] = []
            DATA[ID].append(str(rowdict['associated_resource_id']))
        return(DATA)

    def Warehouse_Resources(self, new_items, item_tags, item_associations):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        now_utc = datetime.now(utc)
        for item in ResourceV2.objects.filter(Affiliation__exact=self.Affiliation):
            self.cur[str(item.LocalID)] = item
        
        for new_id in new_items:
            item = new_items[new_id]
            # Convert warehouse last_update JSON string to datetime with timezone
            # Incoming last_update is a datetime with timezone
            # Once they are both datetimes with timezone, compare their strings
            # Can't compare directly because tzinfo have different represenations in Python and Django
            if not self.args.ignore_dates:
                try:
                    cur_dtm = parse_datetime(self.cur[str(item['id'])].EntityJSON['last_updated'].replace(' ',''))
                except:
                    cur_dtm = datetime.utcnow()
                try:
                    new_dtm = item['last_updated']
                except:
                    new_dtm = None
                if str(cur_dtm) == str(new_dtm):
                    self.stats['Resource.Skip'] += 1
                    continue

            ID = 'urn:glue2:GlobalResource:{}.{}'.format(item['id'], self.Affiliation)
            if 'last_updated' in item and isinstance(item['last_updated'], datetime):
                item['last_updated'] = item['last_updated'].strftime('%Y-%m-%dT%H:%M:%S%z')
            if 'start_date_time' in item and isinstance(item['start_date_time'], datetime):
                item['start_date_time'] = item['start_date_time'].strftime('%Y-%m-%dT%H:%M:%S%z')
            if 'end_date_time' in item and isinstance(item['end_date_time'], datetime):
                item['end_date_time'] = item['end_date_time'].strftime('%Y-%m-%dT%H:%M:%S%z')

            try:
                ProviderID = 'urn:glue2:GlobalResourceProvider:{}.{}'.format(item['provider'], self.Affiliation)
            except:
                ProviderID = None

            try:
                ResourceGroup = item['resource_group']
            except:
                ResourceGroup = None

            try:
                Type = item['resource_type']
            except:
                Type = None

            try:
                QualityLevel = self.fm['record_status'][str(item['record_status'])]
            except:
                QualityLevel = None

            try:
                Keywords = ','.join(item_tags[str(item['id'])])
            except:
                Keywords = None

            try:
                Associations = ','.join(item_associations[str(item['id'])])
            except:
                Associations = None
            
            try:
                model = ResourceV2(ID=ID,
                                    Name = item['resource_name'],
                                    CreationTime = now_utc,
                                    Validity = None,
                                    EntityJSON = item,
                                    Affiliation = self.Affiliation,
                                    ProviderID = ProviderID,
                                    ResourceGroup = ResourceGroup,
                                    Type = Type,
                                    ShortDescription = item['short_description'],
                                    Description = item['resource_description'],
                                    QualityLevel = QualityLevel,
                                    LocalID = str(item['id']),
                                    Topics = item['topics'],
                                    Keywords = Keywords,
                                    Associations = Associations,
                    )
                model.save()
                self.logger.debug('Resource save ID={}'.format(ID))
                self.new[item['id']]=model
                self.stats['Resource.Update'] += 1
            except (DataError, IntegrityError) as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, ID, e.message)
                self.logger.error(msg)
                return(False, msg)

        for id in self.cur:
            if id not in new_items:
                try:
                    ID = 'urn:glue2:GlobalResource:{}.{}'.format(id, self.Affiliation)
                    ResourceV2.objects.get(pk=ID).delete()
                    self.stats['Resource.Delete'] += 1
                    self.logger.info('Resource delete ID={}'.format(ID))
                except (DataError, IntegrityError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, ID, e.message))
        return(True, '')

    def Warehouse_Providers(self, new_items):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        now_utc = datetime.now(utc)
        for item in ResourceV2Provider.objects.filter(Affiliation__exact=self.Affiliation):
            self.cur[str(item.LocalID)] = item
        for new_id in new_items:
            item = new_items[new_id]
            ID = 'urn:glue2:GlobalResourceProvider:{}.{}'.format(item['id'], self.Affiliation)

            try:
                model = ResourceV2Provider(ID=ID,
                                    Name = item['name'],
                                    CreationTime=now_utc,
                                    Validity=None,
                                    EntityJSON=item,
                                    Affiliation=self.Affiliation,
                                    LocalID=str(item['id']),
                    )
                model.save()
                self.logger.debug('ResourceProvider save ID={}'.format(ID))
                self.new[item['id']]=model
                self.stats['ResourceProvider.Update'] += 1
            except (DataError, IntegrityError) as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, ID, e.message)
                self.logger.error(msg)
                return(False, msg)

        for id in self.cur:
            if id not in new_items:
                try:
                    ID = 'urn:glue2:GlobalResourceProvider:{}.{}'.format(id, self.Affiliation)
                    ResourceV2Provider.objects.get(pk=ID).delete()
                    self.stats['ResourceProvider.Delete'] += 1
                    self.logger.info('ResourceProvider delete ID={}'.format(ID))
                except (DataError, IntegrityError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, ID, e.message))
        return(True, '')

    def SaveDaemonLog(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            with open(path, 'r') as file:
                lines=file.read()
                file.close()
                if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                    ts = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                    newpath = '{}.{}'.format(path, ts)
                    shutil.copy(path, newpath)
                    print('SaveDaemonLog as {}'.format(newpath))
        except Exception as e:
            print('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signal, frame):
        self.logger.critical('Caught signal={}, exiting...'.format(signal))
        sys.exit(0)

    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)
        self.logger.info('Starting program={} pid={}, uid={}({})'.format(os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

        while True:
            pa_application=os.path.basename(__file__)
            pa_function='Warehouse_UIUC'
            pa_id = 'resources'
            pa_topic = 'resources'
            pa_about = 'uiuc.edu'
            pa = ProcessingActivity(pa_application, pa_function, pa_id , pa_topic, pa_about)

            if self.src['scheme'] == 'postgresql':
                CURSOR = self.Connect_Source(self.src['uri'])

            self.start = datetime.now(utc)
            self.stats['ResourceProvider.Update'] = 0
            self.stats['ResourceProvider.Delete'] = 0
            self.stats['ResourceProvider.Skip'] = 0
            INPUT = self.Retrieve_Providers(CURSOR)
            (rc, warehouse_msg) = self.Warehouse_Providers(INPUT)
            self.end = datetime.now(utc)
            summary_msg = 'Processed ResourceProvider in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.stats['ResourceProvider.Update'], self.stats['ResourceProvider.Delete'], self.stats['ResourceProvider.Skip'])
            self.logger.info(summary_msg)

            RESTAGS = self.Retrieve_Resource_Tags(CURSOR)
            RESASSC = self.Retrieve_Resource_Associations(CURSOR)
            
            self.start = datetime.now(utc)
            self.stats['Resource.Update'] = 0
            self.stats['Resource.Delete'] = 0
            self.stats['Resource.Skip'] = 0
            INPUT = self.Retrieve_Resources(CURSOR)
            (rc, warehouse_msg) = self.Warehouse_Resources(INPUT, RESTAGS, RESASSC)
            self.end = datetime.now(utc)
            summary_msg = 'Processed Resource in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.stats['Resource.Update'], self.stats['Resource.Delete'], self.stats['Resource.Skip'])
            self.logger.info(summary_msg)

            self.Disconnect_Source(CURSOR)
            
            pa.FinishActivity(rc, summary_msg)
            break

if __name__ == '__main__':
    router = HandleLoad()
    myrouter = router.run()
    sys.exit(0)
