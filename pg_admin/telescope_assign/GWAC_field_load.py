from GWAC_grid_file import GWAC_grid_file

def GWAC_field_load(grid_id):

    grid_input = GWAC_grid_file(grid_id)

    e=open(grid_input,'r')
    p=0
    linese=['0']
    while True:
        linese[p]=e.readline()
        if not linese[p]: break
        if linese[p][0] != "#":
          p=p+1
          linese.append(p)
    e.close()
    linese = list(filter(None,linese))
    maxrece = len(linese)

    match_region = []
    grid_id_arr = []
    field_id_arr = []
    fov_sizex_arr = [] 
    fov_sizey_arr = []    
    ra_center_arr = []
    dec_center_arr = []
    radeg_h1_arr = []
    decdeg_h1_arr = []
    radeg_h2_arr = []
    decdeg_h2_arr = []
    radeg_l1_arr = []
    decdeg_l1_arr = []
    radeg_l2_arr = []
    decdeg_l2_arr = []

    for n in range(len(linese)):
        field_id=(linese[n].split()[1])
        fov_sizex=(linese[n].split()[2])
        fov_sizey=(linese[n].split()[3])
        ra_center=(float(linese[n].split()[4]))
        dec_center=(float(linese[n].split()[5]))
        radeg_h1=(float(linese[n].split()[6]))
        decdeg_h1=(float(linese[n].split()[7]))
        radeg_h2=(float(linese[n].split()[8]))
        decdeg_h2=(float(linese[n].split()[9]))
        radeg_l1=(float(linese[n].split()[10]))
        decdeg_l1=(float(linese[n].split()[11]))
        radeg_l2=(float(linese[n].split()[12]))
        decdeg_l2=(float(linese[n].split()[13]))

        match_region.append('field')
        grid_id_arr.append(grid_id)
        field_id_arr.append(field_id)
        fov_sizex_arr.append(fov_sizex)
        fov_sizey_arr.append(fov_sizey)
        ra_center_arr.append(ra_center)
        dec_center_arr.append(dec_center)
        radeg_h1_arr.append(radeg_h1)
        decdeg_h1_arr.append(decdeg_h1)
        radeg_h2_arr.append(radeg_h2)
        decdeg_h2_arr.append(decdeg_h2)
        radeg_l1_arr.append(radeg_l1)
        decdeg_l1_arr.append(decdeg_l1)
        radeg_l2_arr.append(radeg_l2)
        decdeg_l2_arr.append(decdeg_l2)       

    return [match_region,grid_id_arr,field_id_arr,fov_sizex_arr,fov_sizey_arr,\
    ra_center_arr,dec_center_arr,\
    radeg_h1_arr,decdeg_h1_arr,radeg_h2_arr,decdeg_h2_arr,\
    radeg_l1_arr,decdeg_l1_arr,radeg_l2_arr,decdeg_l2_arr]
