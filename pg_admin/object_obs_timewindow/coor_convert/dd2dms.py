def dd2dms(value_dd):
	dd = int(value_dd)
	mm = int((value_dd - dd)*60.0)
	ss = (((value_dd - dd)*60.0) - mm)*60.0
	if value_dd < 0:
		mm = abs(mm)
		ss = abs(ss)
		dms = "%02d:%02d:%05.2f" % (dd,mm,ss)
	else:
		dms = "%02d:%02d:%05.2f" % (dd,mm,ss)	
	return (dms)

# dms = dd2dms(-0.574541667)
# print dms