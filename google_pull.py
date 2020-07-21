

"""

import boto3
import sys
import os, os.path
from datetime import datetime
from google.cloud import bigquery
import gzip
import logging
from google.api_core.exceptions import NotFound
import io
from awsglue.utils import getResolvedOptions
import multiprocessing
from functools import partial
from ade_utils import athena_utils, io_utils
import traceback
from google.cloud.bigquery.retry import DEFAULT_RETRY



# varibles to validate the arguments
__HIVE_SCHEMA = ['adc_workspace', 'adc_raw']
__DATASET_IDS = ["123960519", "125853882", "16061795", "63256407"]
__SUBJECTS = ['android', 'ios', 'web']

__PROJECT_NAME = "google_analytics"
__PARTITION_COLUMN = "datestamp"

# we fix lists size to be 200, and it fill out with None if needed
__LIMITED_LISTS_SIZE = 200

# usually, hive column names are converted to bigquery using underscores ("_");
# for example, geoNetwork_networkDomain refers to the key networkDomain in the
# geoNetwork dictionary. So, "geoNetwork"."networkDomain" -> "geonetwork_networkdomain"
# Anyway, there are some exceptions as below:
__FIELDS_NAMES_MAPPING_EXCEPTIONS = {
    "date": "session_date"
}
__PROCESS_DIFFERENTLY = "hits"

app_name = 'google_analytics'
__LOG = []
__WARNING_GENERATED = False
batch_size = 15000


def local_log(s):
    """
    Logging function acting as an handler for logging and SMTP

    :param s: string to be logged
    :return:
    """
    logging.info(s)
    __LOG.append(s)


def get_schema_from_hive(subject, hive_schema, data_date, params):
    """Query athena to get the schema for raw (output) layer
    :param subject: should be one of the items in __SUBJECTS
    :param hive_schema: should be one in __HIVE_SCHEMA
    :param data_date: YYYYMMDD data date
    :return: the schema of the hive table, as a list of fields
    """
    print("Started extracting schema from hive")
    table_name = ".".join([hive_schema, "_".join(['%s' % app_name, subject])])
    query = "SELECT * FROM %s WHERE datestamp = 20180701 LIMIT 0" % (table_name)
    client = boto3.client('athena')
    output_directory = io_utils.get_dated_path(params['raw_data_path'], data_date, app_name + '/temp', subject)
    execution = athena_utils.run_query(client, query, hive_schema, output_directory)
    execution_id = execution['QueryExecutionId']
    filename = athena_utils.get_query_result_filename(client, execution_id)
    if filename == False:
        print("Failed to get athena query result filename so exiting")
        sys.exit()
    s3_client = boto3.client('s3', region_name='us-west-2')
    s3_file_path = output_directory + "/" + filename
    bytes_buffer = io.BytesIO()
    s3_client.download_fileobj(Bucket=params['bucket_name'], Key=io_utils.get_s3_relative_path(s3_file_path),
                               Fileobj=bytes_buffer)
    byte_value = bytes_buffer.getvalue()
    str_value = byte_value.decode("utf-8")
    raw_schema = [str(i).strip().replace("\"", "") for i in str_value.split(",")]
    print("Completed extracting schema from hive")
    return raw_schema


def get_table_name(data_date):
    """Construct a Google Sessions table name from passed parameters

    :param data_date: YYYYMMDD data date
    :return: the table name in bigquery
    """
    return "_".join(['ga_sessions', data_date])


def map_input_to_raw_field_names_aux(input_schema, raw_schema, current_field):
    """Auxiliary function to deeply traverse fields in a dict
    """
    res = {}
    for field in input_schema:
        if field in __FIELDS_NAMES_MAPPING_EXCEPTIONS:
            raw_field_name = __FIELDS_NAMES_MAPPING_EXCEPTIONS[field]
        else:
            field = __FIELDS_NAMES_MAPPING_EXCEPTIONS.get(field, field)
            lower_name, type_, schema = input_schema[field]
            raw_field_name = "_".join(current_field + [lower_name])
        if type_ == "LIST":
            # first case: its a list with indices, in which case we flatten it
            if "index" in schema:
                found = False
                for i, raw_field in enumerate(raw_schema):
                    if raw_field.startswith(raw_field_name + "_"):
                        found = True
                        break
                if found:
                    res[field] = (raw_field_name, i, True)
            else:  # or... it is a list of elements like hits
                res[field] = map_input_to_raw_field_names_aux(schema, raw_schema, current_field + [lower_name])
        elif type_ == "DICT":
            res[field] = map_input_to_raw_field_names_aux(schema, raw_schema, current_field + [lower_name])
        else:
            i = raw_schema.index(raw_field_name) if raw_field_name in raw_schema else None
            if i is not None:
                res[field] = (raw_field_name, raw_schema.index(raw_field_name), False)
    return res


def map_input_to_raw_field_names(input_schema, raw_schema):
    """Map the input (bigquery) schema with to expected raw (output) schema

    The mapping is based on the convention that nested fields in the input schema
    are joined with "_" and lowered in the raw schema. For example,
    "hits"."appInfo"."screenName" is mapped to "hits_appinfo_screenname"

    :param input_schema: the bigquery schema
    :param raw_schema: the schema for the raw (output) table, as a list of fields
    :return: a deep dict where the leaves are tuples (raw_schema_field_name,
        position_of_field_in_raw_schema, boolean indicating if the field corresponds
        to a list that should be flatten as in the function 'fill_in_list'))
    """
    return map_input_to_raw_field_names_aux(input_schema, raw_schema, [])


def get_input_schema_aux(schema, res):
    """Auxiliary function
    """
    real_name = schema.name
    lower_name = real_name.lower()
    type_ = schema.field_type
    if type_ == "RECORD":
        if schema.mode == "REPEATED":
            type_ = "LIST"
        else:
            type_ = "DICT"
        res[real_name] = (lower_name, type_, {})
        for elem in schema.fields:
            get_input_schema_aux(elem, res[real_name][2])
    else:
        res[real_name] = (lower_name, type_, None)


def get_input_schema(bigquery_schema):
    """Get a friendly version of the schema of bigquery

    :param bigquery_schema: the original bigquery schema
    :return: a dict having the lower case name of the field as key and
             the value is given by the original name, the type as a string,
             and the schema (when given, like in case of a dict or list)
    """
    res = {}
    for elem in bigquery_schema:
        get_input_schema_aux(elem, res)
    return res


def formatted_value(val):
    """Generate a formmated value by stripping commas off

    :param val: given value
    :return: formatted value
    """
    NoneType = type(None)
    try:
        if isinstance(val, str):
            try:
                value = str(val.encode('utf-8').decode('ascii', 'ignore'))
                return value.replace(',', '|')
            except ValueError as exception:
                return "UNKNOWN"
        else:
            if isinstance(val, NoneType):
                return ""
            else:
                return str(val).replace(',', '|')
    except ValueError as exception:
        print(ValueError)


def fill_in_list(row, first_elem_index, res):
    if row and len(list(row[0].keys())) == 3:
        for elem in row:
            res[first_elem_index + int(elem["index"]) - 1] = formatted_value(elem["customVarName"])
            res[first_elem_index + int(elem["index"]) - 1] = formatted_value(elem["customVarValue"])
    else:
        for elem in row:
            res[first_elem_index + int(elem["index"]) - 1] = formatted_value(elem["value"])


# res = [""] * len(raw_schema)
def process_fields(row, fields_mapping, res, ignore_fields=False):
    if row:
        if type(row) == list:
            if len(row) > 1:
                logging.warning("Field is a list with more than one element; " + \
                                "flattening just the first element: " + str(row))
                global __WARNING_GENERATED
                __WARNING_GENERATED = True
            if type(row[0]) != dict:
                logging.error("Trying to flatten the first element of a list " + \
                              "which is also a list (expecting a dict): " + str(row))
            else:
                row = row[0]
        try:

            for field in row.keys():
                if ignore_fields:
                    if field == __PROCESS_DIFFERENTLY:
                        continue
                if field not in fields_mapping:
                    continue
                if type(fields_mapping[field]) == dict:
                    process_fields(row[field], fields_mapping[field], res)
                else:
                    raw_field_name, raw_index, is_list = fields_mapping[field]
                    if is_list:
                        fill_in_list(row[field], raw_index, res)
                    else:
                        res[raw_index] = formatted_value(row[field])
        except Exception as e:
            logging.error("Unable to process record: " + str(row))
            logging.error(e)


def process_single_row(row, raw_schema, fields_mapping):
    """Process an entire row of bigquery

    :param row: the bigquery row
    :param raw_schema: the schema for the raw (output) table, as a list of fields
    :param input_schema: the bigquery schema
    :param fields_mapping: the mapping between raw and bigquery fields
    :param fd: file descriptor where to write the data to
    """
    output_row = [""] * len(raw_schema)
    process_fields(row, fields_mapping, output_row, ignore_fields=True)
    dwh_timestamp = io_utils.get_date_stamp()
    dwh_timestamp_index = raw_schema.index("dwh_timestamp")
    mycontent = []
    for hit_columns in row["hits"]:
        hits_row = output_row[:]
        process_fields(hit_columns, fields_mapping["hits"], hits_row)
        hits_row[dwh_timestamp_index] = dwh_timestamp
        mycontent.append("%s\n" % ",".join(hits_row).replace('\n', ''))
    return ''.join(mycontent)


def process_rows(rows, table_schema, raw_schema, subject, data_date, params, offset):
    """Process all the rows of bigquery

    The function performs the following steps:
    * Get the input schema from BigQuery and the raw (output) schema from Hive
    * Infer the mapping between the input and raw schemas. For instance, "
        "hits_contentgroup_previouscontentgroup2" is mapped to
        "hits"."contentgroup"."previouscontentgroup2" (with the corresponding letters cases)
    * Process every row one by one:
        * First process all fields but hits, creating the first portion of a row r_1
        * Process hits, which is a list of lists. Ending up with [h_1, h_2, ...]
        * The result is r_1, h_1; r_1, h_2; ... (i.e. every instance of hits is
            concatenated with r_1)
    * Write the output to a local file

    :param row: the bigquery row
    :param table_schema: the schema of the input table
    :param raw_schema: the schema for the raw (output) table, as a list of fields
    :param tmp_file_name: output file to store the results
    """
    print("Started processing rows")
    try:
        input_schema = get_input_schema(table_schema)
        fields_mapping = map_input_to_raw_field_names(input_schema, raw_schema)
        counter = 0
        output = []
        for row in rows:
            if counter % 10000 == 0:  print("Processing line " + str(offset + counter))
            counter += 1
            result = process_single_row(row, raw_schema, fields_mapping)
            output.append(result)
        s3 = boto3.client('s3')

        s3.put_object(Bucket=params['bucket_name'], Key='%s/%s/%s/datestamp=%s/raw_%s_%s.csv.gz' % (
            params['raw_data_path'], app_name, subject, data_date, subject, str(int(offset / batch_size))),
                      Body=gzip.compress(bytes(''.join(output), 'utf-8')))
        del (output)
        print("total records %s" % counter)
        print("\n GA raw completed successfully")
    except Exception as inst:
        print("\n Exception occured processing rows process_rows "+offset)
        print(type(inst))
        print(inst.args)
        print(inst)
        raise
        sys.exit(1)


def process_table(google_dataset_id, raw_schema, subject, data_date, params, offset):
    """Function to process a Google Sessions table on bigquery

    The function performs the following steps:
    * Get the rows from the table
    * Save the output table in a local file
    * Compress the file
    * Move the file to HDFS

    :param client: bigquery client
    :param table: bigquery table got from the client
    :param raw_schema: the schema for the raw (output) table, as a list of fields
    :param google_dataset_id: should be one of the items in __DATASET_IDS
    :param subject: should be one of the items in __SUBJECTS
    :param data_date: YYYYMMDD data date
    :param hdfs_data_path: path to store the output table
    :param hive_schema: should be one in __HIVE_SCHEMA
    :return: error code
    """
    try:
        print("\n Started processing table %s" % offset)
        client = bigquery.Client()
        dataset_ref = client.dataset(google_dataset_id)
        table_ref = dataset_ref.table(get_table_name(data_date))
        table = client.get_table(table_ref)
        maxresults = min(abs(table.num_rows - offset), batch_size)
        counter = 0
        rows = []
        pageToken = None
        # Have to make individual calls based on page token to get correct data otherwise same dataset has been returned from Bigquery for each page
        while True:
            if counter == 0:
                rows_iter = client.list_rows(table, max_results=maxresults, start_index=offset)
            else:
                rows_iter = client.list_rows(table, page_token=pageToken)
            for page in rows_iter.pages:
                for row in list(page):
                    counter = counter + 1
                    rows.append(row)
                    if counter == maxresults:
                        break
                pageToken = rows_iter.next_page_token
                break
            if counter == maxresults:
                print("Counter reached maxresults %s" % maxresults)
                break
            if pageToken is None:
                break
        process_rows(rows, table.schema, raw_schema, subject, data_date, params, offset)
        del (rows)
        client.close()
        if __WARNING_GENERATED:
            print("Warning: flattening list with more than one element", \
                  data_date, subject)
    except Exception as exp:
        print("%s" % sys.exc_info()[1])
        print("Error processing GA rows process_table")
        traceback.print_exc()
        raise
        sys.exit(1)
        return 1
    return 0


def main(google_dataset_id, subject, data_date):
    """Main Processing

    Checks if the data is already available in Bigquery, if not it fails

    :param google_dataset_id: should be one of the items in __DATASET_IDS
    :param subject: should be one of the items in __SUBJECTS
    :param data_date: YYYYMMDD data date
    :param hdfs_data_path: path to store the output table
    :param hive_schema: should be one in __HIVE_SCHEMA
    :return: error code
    """
    try:
        params = io_utils.get_db_and_dir_details()
        conf_file = '%s/auction_credential.json' % io_utils.get_app_conf_path(app_name)
        save_as = 'auction_credential.json'
        s3 = boto3.client('s3')
        bucket_name = params['bucket_name']
        hive_schema = params['raw_database']
        s3.download_file(params['bucket_name'], io_utils.get_s3_relative_path(conf_file), save_as)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = save_as
        client = bigquery.Client()
        dataset_ref = client.dataset(google_dataset_id)
        table_ref = dataset_ref.table(get_table_name(data_date))
        table_found = False
        if not table_found:
            try:
                table = client.get_table(table_ref)
                table_found = True
                print("Table  found")
            except NotFound as nf:
                print("Table not found")
                sys.exit()
        if table_found:
            raw_schema = get_schema_from_hive(subject, hive_schema, data_date, params)
            if raw_schema is None:
                return 0
            pool = multiprocessing.Pool(processes=2)
            myoffsets = [x for x in range(0, table.num_rows, batch_size)]
            print("Total rows %s " % table.num_rows)
            print(myoffsets)
            client.close()
            del(client)
            prod_x = partial(process_table, google_dataset_id, raw_schema, subject, data_date, params)
            result = pool.map(prod_x, myoffsets)
            pool.close()
            pool.join()
            athena_client = boto3.client("athena")
            output_directory = io_utils.get_dated_path(params['raw_data_path'], data_date, app_name + '/temp', subject)
            athena_utils.run_query(athena_client,
                                   io_utils.get_alter_partition_statement(subject, hive_schema, data_date, app_name),
                                   hive_schema, output_directory)
            print("Altering Schema Complete")
            return 1
    except Exception as inst:
        print("\n Exception occured processing rows %s %s %s main" % (type(inst), inst.args, inst))
        traceback.print_exc()
        raise
        sys.exit(1)
        return 0


def get_args():
    try:
        args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
        workflow_name = args['WORKFLOW_NAME']
        workflow_run_id = args['WORKFLOW_RUN_ID']
        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                                                  RunId=workflow_run_id)["RunProperties"]
        args['data_date'] = workflow_params['--data_date']
        args['subject'] = workflow_params['--subject']
        args['dataset_id'] = workflow_params['--dataset_id']
        print("Args %s" % args)
    except:
        print("Exception occured while parsing arguments")
        args = getResolvedOptions(sys.argv,
                                  ['dataset_id', 'data_date', 'subject'])
    return args


if __name__ == '__main__':
    START_TIME = datetime.now()
    print("BOTO3 Version  : " + str(boto3.__version__))
    glue_client = boto3.client("glue")
    args = get_args()
    glue = boto3.client(service_name='glue', region_name='us-west-2',
                        endpoint_url='https://glue.us-west-2.amazonaws.com')
    try:
        if main(args['dataset_id'], args['subject'], args['data_date']):
            END_TIME = datetime.now()
            print(
                    "Google 360 - Dataset %s for date %s and suite %s Completed! End time = %s , Time taken = %s" %
                    (args['dataset_id'], args['data_date'],  args['subject'],
                     str(END_TIME), str(END_TIME - START_TIME)))
        else:
            print("Error processing GA raw")
    except Exception as exp:
        print("exception from main")
        print(exp)
        traceback.print_exc()
        local_log("%s" % sys.exc_info()[1])
        raise
        return_code = 0
