#!./venv/bin/python

# colony.py
# a hive ddl generator
#
#
# authors:  klopes, jstephens, mdurisheti
# created:  2/12/2016 (forked from CompareTables.py)

import mysql.connector
import re
import time

# configuration
#HOST = 'pdb60-uswest2-edw.cluster-custom-c7svivynpdco.us-west-2.rds.amazonaws.com'
# HOST = 'qdb60-uswest2-cluster.cluster-czyqklvijc7w.us-west-2.rds.amazonaws.com'
# PORT = 3306
# USER_NAME = 'app_dlakedms_ro'
# PASSWORD = 'Tessting3'
# MYSQL = 'mysql'
# INFORMATION_SCHEMA = 'information_schema'
# APPLICATION = 'morpheus'
# PREFIX_SHORT_NAME = 'uaadb'
# LOCATION_ROOT = '/adc-dev/data'
# TARGET = 'adc_clean'
# TARGET_FLDR = 'clean'



HOST = '99.99.99.999'
PORT = 1433
USER_NAME = 'user_name'
PASSWORD = 'password'
MYSQL = 'mysql'
INFORMATION_SCHEMA = 'information_schema'
APPLICATION = 'dw'
PREFIX_SHORT_NAME = 'dw'
LOCATION_ROOT = '/xxx/xxx'
TARGET = 'db_name'
TARGET_FLDR = 'folder_name'


MYSQL_TYPE_MAP = \
    {
        # mysql type : hive type
        'varchar' : 'string',
        'text' : 'string',
        'char' : 'string',
        'tinytext' : 'string',
        'longtext' : 'string',
        'enum' : 'string',
        'time' : 'string',
        'set' : 'string',
        'bigint' : 'bigint',
        'mediumint' : 'int',
        'int' : 'int',
        'integer' : 'int',
        'float' : 'float',
        'double' : 'double',
        'smallint' : 'smallint',
        'tinyint' : 'tinyint',
        'boolean' : 'boolean',
        'decimal' : 'double',
        'date' : 'string',
        'datetime' : 'timestamp',
        'timestamp' : 'timestamp',
        'blob' : 'binary',
        'varbinary' : 'binary'
    }

NUMERAL_MAP = \
    {
        '1' : 'first',
        '2' : 'second',
        '3' : 'third',
        '4' : 'fourth',
        '5' : 'fifth',
        '6' : 'sixth',
        '7' : 'seventh',
        '8' : 'eighth',
        '9' : 'ninth',
        '10' : 'tenth',
        '11' : 'eleventh',
        '12' : 'twelfth',
        '20' : 'twenty',
        '30' : 'thirty',
        '40' : 'forty',
        '50' : 'fifty',
        '60' : 'sixty',
        '70' : 'seventy',
        '80' : 'eighty',
        '90' : 'ninety'
    }

now = time.strftime('%Y-%m-%d %H:%M:%S')

class SchemaGenerator:
    '''
    A table DDL generator for hive.
    '''
    def __init__(self, host, port, user, password, application, prefix_short_name, target=TARGET, target_fldr=TARGET_FLDR,
                 source_type=MYSQL, information_schema=INFORMATION_SCHEMA, fully_qualified=False):
        self.connection = mysql.connector.connect(host=host, port=port, user=user,
                                                  password=password, database=information_schema)
        self.connection2 = mysql.connector.connect(host=host, port=port, user=user,
                                                  password=password, database=information_schema)
        self.cursor = self.connection.cursor()
        self.cursor2 = self.connection2.cursor()
        self.mysql_map = {}
        self.numeral_map = NUMERAL_MAP
        self.application = application
        self.prefix_short_name = prefix_short_name
        prefix_long_form = '%s_%s' % (application, prefix_short_name)
        self.prefix = prefix_long_form if fully_qualified else prefix_short_name
        self.target = target
        self.target_fldr = target_fldr
        self.source_type = source_type
        self.table_comments = {}

    def __enter__(self):
        return self

    def __exit__(self, *err):
        self.cursor.close()

    def _mysql_query(self, database, table, target_name):
        '''
        Reads source mysql information_schema table information to a map.
        :param table: The source table as a string
        :param schema: The source database as a string
        '''
        query = 'select column_name, ordinal_position, data_type, column_comment from information_schema.columns  where table_schema=\''\
                + database + '\' and table_name=\'' + table + '\''
        print "printing query: %s" %(query)
        self.cursor.execute(query)
        print self.cursor
        self.mysql_map[table] = []
        for (column_name, ordinal_position, data_type, column_comment) in self.cursor:
            self.mysql_map[table].append((column_name, ordinal_position, data_type, column_comment, database, target_name))
        print self.mysql_map



    def _mysql_tablecomment_query(self, database, table):
        '''
        Reads source mysql information_schema table information to get tables comment (metadata about the table).
        :param table: The source table as a string
        :param schema: The source database as a string
        '''
        query2 = 'select table_comment from information_schema.tables  where table_schema=\''\
                + database + '\' and table_name=\'' + table + '\''
        print "printing table comment query: %s" %(query2)
        self.cursor2.execute(query2)
        print self.cursor2
        self.table_comments[table]=[]
        for (tc) in self.cursor2:
            self.table_comments[table].append((tc)) 
            print self.table_comments


    def _transform_field_name(self, field_name):
        '''
        Transforms invalid field names to hive-friendly strings.
        Currently traps for:
            - field name starts with _
            - field name starts with numeral
        :param field_name: source field name as string
        :return: transformed field name as string
        '''
        if field_name is None or len(field_name) == 0: return field_name
        pattern = '(\d+)(st|nd|rd|th)(.*)'
        result = re.match(pattern, field_name)
       # print result
        if result is None or result == '':
            #Test for ordinal
            pattern = '(\d+)(.*)'
            result = re.match(pattern, field_name)
            if result is None or result == '':
                if field_name.find('_') == 0: # Fix columns that start with an underscore
                    return field_name[1:]
                return field_name
            else:
                digit = result.group(1)
                rest = result.group(2)
                digit_value = self.numeral_map[digit]
                return digit_value + '' + rest
        digit = result.group(1)
        rest = result.group(3)
        digit_value = self.numeral_map[digit]
        if digit_value == None or digit_value == '':
            error = 'Invalid digit %d' % (digit_value)
            raise Exception(error)
        else:
            value = digit_value + '' + rest

        return value

    def _esc_spl_char(self, inp_str):
        '''
        Escapes the listed special characters in a string.
        '''
        sc_list = [";","`",",","\"","'"]
        for sc in sc_list:
            inp_str = inp_str.replace(sc, '\\' + sc)

        return inp_str

    def get_create_table(self, source_table_name):
        '''
        Builds a CREATE EXTERNAL TABLE hive DDL statement for a source table.
        :param source_table_name:
        :return: a ddl command as string
        '''
        if not source_table_name in self.mysql_map:
            raise Exception('NoTableFound','Table by name ' + source_table_name \
                            + ' not found. Please run compare or _mysql_query before generate  ')

        #location = 's3:/%s/%s/%s/%s' % (LOCATION_ROOT, self.target_fldr, self.application, self.prefix_short_name)
        #location = 's3://{{io_utils.get_bucket_name()}}/%s/%s/%s' % (self.target_fldr, self.application, self.prefix_short_name)
        location = 's3://{{io_utils.get_bucket_name()}}/data/%s/%s/%s' % (self.target_fldr, self.application, 'resi_sellerdashboard')
        print "printing location: %s" %(location)
        data = self.mysql_map[source_table_name]
        print "printing data: %s" %(data)
        sorted_data = sorted(data, key = lambda record:record[1])
        print "printing sorted_data: %s" %(sorted_data)
        target_name = sorted_data[0][5]
        print "printing target_name: %s" %(target_name)
        self.prefix = 'resi_sellerdashboard'
        table_name = '%s_%s' % (self.prefix, target_name)
        tc_data = self.table_comments[source_table_name]
        print "printing tc data: %s" %(tc_data)
        tbl_comment = tc_data[0][0]
        print "printing tbl_comment: %s" %(tbl_comment)
        # cmd = 'DROP TABLE IF EXISTS %s.%s;\n' % (self.target, table_name)
        cmd = ''
        cmd += 'CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s(\n' % (self.target, table_name)
        for tuple in sorted_data:
            field_name = self._transform_field_name(tuple[0].lower())
            cmd += '    %s ' % field_name
            hive_type = MYSQL_TYPE_MAP[tuple[2].lower()]
            cmd += hive_type
            cmd += ' COMMENT "%s"' % (self._esc_spl_char(tuple[3]))
            #if tuple[1] < len(sorted_data) :
            #    cmd += ','
            cmd += ',\n'
            #cmd += '\n'
        cmd += '    dwh_timestamp timestamp\n'
        cmd += ')\n'
        cmd += 'COMMENT \'%s\'\n' % (self._esc_spl_char(tbl_comment))
        cmd += 'STORED AS PARQUET\n'
        cmd += 'LOCATION\n'
        cmd += '  \'' + location + '/' + target_name + '\';\n'

        return cmd

    def source_table(self, database, table, target_name):
        '''
        Adds a source table to the stack.
        :param database:
        :param table:
        :return:
        '''
        print(table)
        self._mysql_query(database, table, target_name)
        self._mysql_tablecomment_query(database, table)


    def print_ddl(self, to_file=False, call=get_create_table):
        """
        Generate a concatenated series of DDL statements.
        """
        for table in self.mysql_map:
            #cmd = 'USE %s;\n\n' % self.target
            cmd = ''
            cmd += '--------------------------------------------------------------------------------\n'
            cmd += call(self, source_table_name=table)
            cmd += '\n\n'
            if to_file:
                fn = self.prefix + '_' + table + '.q'
                with open(fn,'w') as f:
                    print("printing cmd")
                    print(cmd)
                    f.write(cmd.encode("utf-8"))
                    print("after write")
                    f.close()
                    print("close")
        else:
            print cmd


if __name__ == '__main__':

    with SchemaGenerator(host=HOST, port=PORT, user=USER_NAME, password=PASSWORD,
                         target=TARGET, application=APPLICATION, prefix_short_name=PREFIX_SHORT_NAME,
                         fully_qualified=False) as g:
        # g.source_table('uaadb', 'authority', 'authority')
        # g.source_table('uaadb', 'lookup_reference', 'lookup_reference')
        # g.source_table('uaadb', 'oauth_client_details', 'oauth_client_details')
        # g.source_table('uaadb', 'oauth_code', 'oauth_code')
        # g.source_table('uaadb', 'party', 'party')
        # g.source_table('uaadb', 'party_address', 'party_address')
        # g.source_table('uaadb', 'party_address_usage', 'party_address_usage')
        # g.source_table('uaadb', 'party_broker_license', 'party_broker_license')
        # g.source_table('uaadb', 'party_external_identifier', 'party_external_identifier')
        # g.source_table('uaadb', 'party_media', 'party_media')
        # g.source_table('uaadb', 'party_person', 'party_person')
        # g.source_table('uaadb', 'party_phone', 'party_phone')
        # g.source_table('uaadb', 'party_preference', 'party_preference')
        # g.source_table('uaadb', 'party_relationship', 'party_relationship')
        # g.source_table('uaadb', 'party_role', 'party_role')
        # g.source_table('uaadb', 'party_web_communication', 'party_web_communication')
        # g.source_table('uaadb', 'role', 'role')
        # g.source_table('uaadb', 'role_authority', 'role_authority')
        # g.source_table('uaadb', 'schema_version', 'schema_version')
        # g.source_table('uaadb', 'tenx_user', 'tenx_user')
        # g.source_table('uaadb', 'token_blacklist', 'token_blacklist')
        # g.source_table('uaadb', 'user_blacklist', 'user_blacklist')
        # g.source_table('uaadb', 'user_invitation_token', 'user_invitation_token')
        # g.source_table('uaadb', 'user_last_login', 'user_last_login')
        # g.source_table('uaadb', 'user_login_source', 'user_login_source')
        # g.source_table('uaadb', 'user_name_reset', 'user_name_reset')
        # g.source_table('uaadb', 'user_password_reset', 'user_password_reset')
        # g.source_table('uaadb', 'user_role', 'user_role')
        # g.source_table('uaadb', 'user_status_history', 'user_status_history')
        g.source_table('resi_sellerdashboard', 'bid_info', 'bid_info')
        g.source_table('resi_sellerdashboard', 'offer', 'offer')
        g.source_table('resi_sellerdashboard', 'property', 'property')
        g.source_table('resi_sellerdashboard', 'property_notes', 'property_notes')
        g.source_table('resi_sellerdashboard', 'property_reo', 'property_reo')
        g.source_table('resi_sellerdashboard', 'schema_version', 'schema_version')
        g.source_table('resi_sellerdashboard', 'sellerdashboard_request', 'sellerdashboard_request')
        g.print_ddl(to_file=True)
