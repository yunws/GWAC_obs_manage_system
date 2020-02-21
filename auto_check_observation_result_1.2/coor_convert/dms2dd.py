def dms2dd(value_dms):
	value_dd = float(value_dms.split(":")[0])+float(value_dms.split(":")[1])/60.0+float(value_dms.split(":")[2])/3600.0
	return (value_dd)

# dec_dd = dms2dd('117:34:28.35')
# print dec_dd