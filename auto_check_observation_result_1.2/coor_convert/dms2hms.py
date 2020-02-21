def dms2hms(value_dms):
	value_dd = float(value_dms.split(":")[0])+float(value_dms.split(":")[1])/60.0+float(value_dms.split(":")[2])/3600.0
	value_hh = value_dd / 15.0
	hh = int(value_hh)
	mm = int((value_hh - hh)*60.0)
	ss = (((value_hh - hh)*60.0) - mm)*60.0
	hms = "%02d:%02d:%05.2f" % (hh,mm,ss)	
	return (hms)

hms = dms2hms('117:34:28.35')
print hms