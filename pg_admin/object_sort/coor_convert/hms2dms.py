def hms2dms(value_hms):
	value_dd = float(value_hms.split(":")[0])* 15.0+float(value_hms.split(":")[1])/60.0* 15.0+float(value_hms.split(":")[2])/3600.0* 15.0
	dd = int(value_dd)
	mm = int((value_dd - dd)*60.0)
	ss = (((value_dd - dd)*60.0) - mm)*60.0
	dms = "%02d:%02d:%05.2f" % (dd,mm,ss)	
	return (dms)

dms = hms2dms('07:50:17.89')
print dms