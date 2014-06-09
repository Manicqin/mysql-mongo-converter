import MySQLdb
from colorama import *
from locale import setlocale, format, LC_ALL
import banner


def group(x, frmt="%d"):
    return format(frmt, x, grouping=True)


def pos(x, y):
    return '\x1b[' + str(y) + ';' + str(x) + 'H'


def create_printer(printer_name):
    def print_index(data):
        print pos(10, 3), [item for item in data if item['Key_Type'] == 'PRIMARY']

    def print_columns(data):
#		filtered = [item for item in data if '_id' in item['Field'] and '_id' != item['Field']]
        print pos(10, 6), data

    def print_props(data):
        print pos(10, 9), "Number Of lines in Table:", group(data["tableNumberOfLines"]), "Number Of Segments:", group(data["segmentsCount"]), "ids:", data["lowBound"], "-", data["highBound"]
        create_printer._number_of_iterations = data['segmentsCount']

    def print_iter(data):
        print pos(10, 10), 'Iteration: ', data['iteration'],\
            'Lines:', group( data['start'] ), '-', group(data['end']),\
            group((1.0 * data['iteration']) / create_printer.numberOfIterations*100, "%.3f")+'%'

    def print_row(data):
        print pos(10, 11), 'Id:', data['id']

    def print_def(data):
        print pos(10, 12), data

    def print_database(data):
        print pos(20, 20),  banner.horizontal(data)

    printer = {"INDEX": print_index,
               "COLUMNS": print_columns,
               "PROP": print_props,
               "ITERATION": print_iter,
               "ROW": print_row,
               "DB": print_database,
               "DEFAULT": print_def}

    if printer_name in printer:
        return printer[printer_name]
    else:
        return print_def["DEFAULT"]
create_printer.numberOfIterations = -1


def clear_screen():
    print (chr(27) + "[2J")


def ascii_gui(data):
    if ascii_gui._first:
        clear_screen()
        init()
        setlocale(LC_ALL, 'en_US')
        ascii_gui._first = False
    create_printer(data["type"])(data["data"])
ascii_gui._first = True


def null_print(data):
    pass


class Source():
    def __init__(self):
        """


        """
        pass


class SqlSource(Source):
    def __init__(self):
        """


        """

    def init(self, connection_data, table_name, output_callback=null_print):
        self._db = MySQLdb.connect(*connection_data)
        self._table_name = table_name
        self._output_callback = output_callback

    def close(self):
        self._db.close()

    def iterate(self, start_with=None, end_with=None):

        """
            Iterate over the rows.
            :return: a dictionary depicting a row.

        """
        cur = MySQLdb.cursors.DictCursor(self._db)

        cur.execute(
            'SELECT MAX(id) , MIN(id) , COUNT(id)  FROM ' +
            self._table_name)

        count = cur.fetchone()
        low_bound = start_with
        high_bound = end_with

        if not start_with:
            low_bound = count['MIN(id)']
        if not end_with:
            high_bound = count['MAX(id)']
        print "startWith", start_with
        line_count = count['COUNT(id)']
        if line_count < 1000:
            rows_per_seg = 1
        else:
            rows_per_seg = 1000

        number_of_segments = (high_bound - low_bound) / rows_per_seg
        self._output_callback({"type": "PROP",
                              "data": {"tableNumberOfLines": line_count,
                                       "lowBound": low_bound,
                                       "highBound": high_bound,
                                       "segmentsCount": number_of_segments}})
        row_number = 0

        for iDx in range(0, number_of_segments+1):

            query_affected_lines = cur.execute(
                'SELECT * FROM ' + self._table_name + ' WHERE ' + 'id between ' +
                str(low_bound) + ' and ' + str(low_bound + rows_per_seg))

            for row in cur.fetchall():
                self._output_callback({"type": "ROW", "data": row})
                yield row

            self._output_callback({"type": "ITERATION",
                                  "data": {"start": row_number,
                                           "end": row_number+rows_per_seg,
                                           "iteration": iDx,
                                           "numberOfLinesInQuery": query_affected_lines}})
            row_number += rows_per_seg
            low_bound = low_bound + rows_per_seg + 1

    def headers(self):

        """
            returns the header of the table

        :return: list of strings
        """
        cur = MySQLdb.cursors.DictCursor(self._db)

        cur.execute('SHOW COLUMNS FROM ' + self._table_name)

        columns = []
        for iDx in cur.fetchall():
            columns.append(iDx['Field'])

        self._output_callback({'type': 'COLUMNS', 'data': columns})
        return columns

    connection = None
    _db = None
    _table_name = None
    _output_callback = None
'''def iterateSql(connectionData , tableName , output_callback):
	db = MySQLdb.connect(*connectionData)

	cur = MySQLdb.cursors.DictCursor(db)

	cur.execute('SHOW INDEX FROM ' + tableName);

	iDxs = [];
	for iDx in cur.fetchall():
		if 'Key_name' in iDx:
			#if iDx['Key_name'] == 'PRIMARY':

			iDxs.append({'Column_name':iDx['Column_name'],'Key_Type':iDx['Key_name']});
	output_callback({"type":"INDEX","data":iDxs});

	cur.execute('SHOW COLUMNS FROM ' + tableName);

	columns = [];
	for iDx in cur.fetchall():
		columns.append(iDx);

	output_callback({'type':'COLUMNS','data':columns});
#	exit()
	cur.execute('SELECT MAX(id) , MIN(id) , COUNT(id)  FROM ' + tableName)

	count = cur.fetchone()

	lineCount = count['COUNT(id)']
	lowBound = count['MIN(id)']
	highBound = count['MAX(id)']

	if lineCount < 1000:
		 rowPerSeg = 1;
	else:
		rowPerSeg = 1000
	segmentsCount = lineCount / rowPerSeg
	output_callback({"type":"PROP","data":{"tableNumberOfLines":lineCount,"lowBound":lowBound,"highBound":highBound,"segmentsCount":segmentsCount}});

#	print pos(10,9) , group( lineCount) , group( segmentsCount )
#	percent =  (1.0 / segmentsCount) * 100
	rowNumber = 0

	for iDx in range(0 , segmentsCount):

		queryAffectedLines = cur.execute('SELECT * FROM ' + tableName + ' WHERE ' + 'id between '+  str(lowBound)  + ' and ' + str(lowBound + rowPerSeg) )

		for row in cur.fetchall():
			output_callback({"type" : "ROW" , "data" : row})
			yield row

		output_callback({"type":"ITERATION","data":{"start":rowNumber,"end":rowNumber+rowPerSeg,"iteration":iDx,"numberOfLinesInQuery":queryAffectedLines}});
		rowNumber = rowNumber + rowPerSeg
		lowBound = lowBound + rowPerSeg

connection = ('172.24.1.239', 'boaztest', '1qazXSW@', 'tracker')

a = SqlSomurce()
a.init(connection, "products", null_print)
print a.headers()
a.close()
# output_callback({"type":"INDEX","data":{"aaa":1,"bbb":2}});


# try:
#	connection = ('172.24.1.239' , 'boaztest' , '1qazXSW@' , 'tracker')
#	tableName = 'tracker.tracker'


#	for item in iterateSql(connection , tableName , null_print):
#		print item
# except (RuntimeError , TypeError , NameError , KeyError)  as err:

#	print (chr(27) + "[2J")
#	print err
#print (pos(10 , 10) + "blabla")'''
