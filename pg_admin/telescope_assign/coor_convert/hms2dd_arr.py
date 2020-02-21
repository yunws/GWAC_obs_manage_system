import numpy as np
def hms2dd_arr(value_hms_arr):
	# print len(value_hms_arr)
	value_dd_arr = []
	for i in range(len(value_hms_arr)):
		value_hms = value_hms_arr[i]
		value_dd = float(value_hms.split(":")[0])* 15.0+float(value_hms.split(":")[1])/60.0* 15.0+float(value_hms.split(":")[2])/3600.0* 15.0
		value_dd_arr.append(value_dd)
	value_dd_arr = np.array(value_dd_arr)
	return (value_dd_arr)

# ra_dd = hms2dd_arr(['07:50:17.89'])
# print ra_dd