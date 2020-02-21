import sys
# from jdcal import gcal2jd
import os
from os.path import exists, join
import os.path
from os import pathsep
import datetime
import time
# load the adapter
import psycopg2
# load the psycopg extras module
import psycopg2.extras
try:
    sys.path.append("./jdcal/")
    from jdcal import gcal2jd
    from jdcal import jd2gcal
except:
    print("please install jdcal code ")
    sys.exit()
from func_object_sort import func_object_sort #``
import numpy as np
import ephem
from datetime import timedelta
from astropy.table import Table,Column
import pandas as pd
import io
import csv


def read(file_location):
    with open(file_location, 'r+', newline='') as csv_file:
        reader = csv.reader(csv_file)
        return [row for row in reader]

def te(fes):
    a = 1
    return(fes)
    b = 2
    print(a + b)

row = [[1,'a',3,23.0],[2,'b',4,23.0],[3,'b',5,23.0],[4,'d',6,23.0],[5,'c',6,23.0]]
rows_np = np.array(row)
rows_table = Table(rows_np, names=('obj_id', 'tw_begin', 'objra', 'objdec'),dtype=('i','a19','f', 'f'))
# rows_sent = Table(names=('obj_id', 'tw_begin', 'objra', 'objdec'),dtype=('i','a19','f', 'f'))
rows_new = Table(names=('obj_id', 'tw_begin', 'objra', 'objdec'),dtype=('i','a19','f', 'f'))
# rows_test = Table(names=('obj_id', 'tw_begin'),dtype=('i','a19'))
rows_test = Table()

row_index = np.where(rows_table['tw_begin'] == 'b')[0]
rows_sent = rows_table[row_index]
print(rows_sent)
rows_table.remove_rows(row_index)
print('last obj id',rows_table[-1]['obj_id'])

# rows_test = rows_table['obj_id'],rows_sent['tw_begin']
rows_test.add_column(rows_table['obj_id'])
rows_test.add_column(rows_table['tw_begin'])
print('rows_test',rows_test)

sent_count = len(rows_sent)
for n in range(sent_count):
    rows_new.add_row(rows_sent[n])
print(rows_new['obj_id'])

data_io = io.StringIO()
# writer = csv.writer(data_io, delimiter='\t')
for n in range(len(row)):
	data_io.write("""test%s\t%s\t%s\t%s\n""" % (row[n][0], row[n][1],row[n][2],row[n][3]))

data_io.seek(0)
# written_value = read(data_io)
test = data_io.read()
test1 = te(1)
print(test1)


