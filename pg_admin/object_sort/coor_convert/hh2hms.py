def hh2hms(value_hh):
	hh = int(value_hh)
	mm = int((value_hh - hh)*60.0)
	ss = (((value_hh - hh)*60.0) - mm)*60.0
	hms = "%02d:%02d:%05.2f" % (hh,mm,ss)	
	return (hms)

hms = hh2hms(117.574541667)
print hms