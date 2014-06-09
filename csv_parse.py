import csv

import json
from pymongo import MongoClient
import sys

import mySqlConnection
import os.path


class CsvSource(mySqlConnection.Source):
    def __init__(self):
        """


        """

        mySqlConnection.Source.__init__(self)

    def init(self, file_name):
        #'csv/' + db_name + '.csv'
        with open(file_name, 'r') as csvfile:
            self._reader = csv.reader(csvfile, dialect='excel')

    def iterate(self):
        yield self._reader.next()

    def close(self):
        pass

    def headers(self):
        return_value = self._reader.next()
        return return_value

    _reader = None

#print 'Number of arguments:', len(sys.argv), 'arguments.'
#print 'Argument List:', str(sys.argv)

if len(sys.argv) < 2:
    exit()


def null_print(data):
    pass


def convert2unicode(mydict):
    for k, v in mydict.iteritems():
        if isinstance(v, str):
            mydict[k] = unicode(v, errors='replace')
        elif isinstance(v, dict):
            convert2unicode(v)


def fix_headers(in_headers):
    headers_fixed = list(in_headers)
    while True:

        index = 0
        for headers_item in in_headers:
            print index, headers_item
            index += 1
        try:
            key = input('Enter iDx id (Enter to continue) ')
        except SyntaxError:
            break
        headers_fixed[key] = raw_input('Enter new name (surround with \"\") ')
    return dict(zip(in_headers, headers_fixed))

def create_source(data,table_name):
    return_source = None
    print data
    if data["type"] ==  "sql":
        return_source = mySqlConnection.SqlSource()
        conn = data["connection"]
        connection = (conn["ip"],conn["user"], conn["pass"], conn["db"])
        return_source.init(connection, table_name, mySqlConnection.ascii_gui)
    elif data["type"] == "csv":
        return_source = CsvSource()
        return_source.init(table_name)
    return return_source

db_name_list = sys.argv[1:]
client = MongoClient()
collections = {}
usingHeaderFile = False
configurations = {}
source_config = {}

if os.path.exists('config'):
    with open('config', 'r') as f:
        config_file = json.load(f)
        configurations= config_file["tables"]
        source_config = config_file["source"]
        usingHeaderFile = True
db = client.tracker2

for db_name in db_name_list:

    source = create_source(source_config,db_name);
    #	mySqlConnection.ascii_gui({'type':'DB','data': db_name})
    if usingHeaderFile is False:
        headers = fix_headers(source.headers)

        # Find which of the fields should be embeded \ referenced
        calculated_fields = []
        for field in headers.values():
            if '_id' in field and '_id' != field:
                calculated_fields.append(field)

        configurations[db_name] = {"headers": headers, "linked": calculated_fields}
    #collections[db_name] = db[db_name]

    #dropping the collection , this is for debug
    #collections[db_name].drop()
    mySqlConnection.clear_screen()

    source.close()

with open('headerCnfg', 'w') as f:
    json.dump(configurations, f)
for db_name in db_name_list:
    mySqlConnection.clear_screen()

    mySqlConnection.ascii_gui({'type': 'DB', 'data': db_name})
    source = create_source(source_config,db_name);
    headers = configurations[db_name]["headers"];
    linked = configurations[db_name]["linked"];
    #	print headers , linked

    startWith = None
    lastDoc = db[db_name].find().sort("_id", -1).limit(1);
    for item in lastDoc:
        startWith = item['_id'] + 1
    print startWith
    for row in source.iterate(startWith):
        row_dict = dict()
        for item in row:
            row_dict[headers[item]] = row[item]
        #fixed_rows = zip(headers.values(),row.values())
        #row_dict = dict(fixed_rows)

        convert2unicode(row_dict)
        for field in linked:
            ref_DB_name = field.split('_')[0]

            if field in row_dict:
                #print ' -------> ' , ref_DB_name , row_dict[field]
                for reference in db[ref_DB_name].find({'_id': row_dict[field]}):
                    #print json.dumps(reference)
                    row_dict[ref_DB_name] = reference
                del row_dict[field]
        #print row_dict
        #print '-------------------------------------------'
        db[db_name].insert(row_dict)
    #print(db_name , collections[db_name].count())
#		if iDx == limit:
#			break
#		iDx = iDx + 1
#	with open('csv/' + db_name  + '.csv', 'r') as csvfile:
#		reader = csv.reader(csvfile, dialect='excel')
#		header = reader.next()
#
#		header = fix_headers(header);
#		continue;
#		for row in reader:
#
#			row_csv =  zip(header,row)
#			row_dict = dict(row_csv)
#			for field in calculated_fields:
#				ref_DB_name = field.split('_')[0]
#				if field in row_dict:
#					print ' -------> ' , ref_DB_name , row_dict[field]
#					for  reference in collections[ref_DB_name].find({'_id':row_dict[field]}):
#						#print json.dumps(reference)
#						row_dict[ref_DB_name] = reference;
#					del row_dict[field]
#			print row_dict
#			print '-----------------------------------'
#			collections[db_name].insert(row_dict)
#	print(db_name , collections[db_name].count())
