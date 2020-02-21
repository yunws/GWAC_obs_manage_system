def hms2dd(value_hms):
	value_dd = float(value_hms.split(":")[0])* 15.0+float(value_hms.split(":")[1])/60.0* 15.0+float(value_hms.split(":")[2])/3600.0* 15.0
	return (value_dd)

# ra_dd = hms2dd('07:50:17.89')
# print ra_dd