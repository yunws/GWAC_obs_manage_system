import numpy as np
def dms2dd_arr(value_dms_arr):
	# print len(value_dms_arr)
	value_dd_arr = []
	for i in range(len(value_dms_arr)):
		value_dms = value_dms_arr[i]
		value_dd = float(value_dms.split(":")[0])+float(value_dms.split(":")[1])/60.0+float(value_dms.split(":")[2])/3600.0
		value_dd_arr.append(value_dd)
	value_dd_arr = np.array(value_dd_arr)
	return (value_dd_arr)

# dec_dd = dms2dd('117:34:28.35')
# print dec_dd