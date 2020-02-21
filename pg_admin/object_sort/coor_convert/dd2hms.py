def dd2hms(value_dd):
	value_hh = value_dd / 15.0
	hh = int(value_hh)
	mm = int((value_hh - hh)*60.0)
	ss = (((value_hh - hh)*60.0) - mm)*60.0
	hms = "%02d:%02d:%05.2f" % (hh,mm,ss)	
	return (hms)

# hms = dd2hms(343.133583)
# print(hms)