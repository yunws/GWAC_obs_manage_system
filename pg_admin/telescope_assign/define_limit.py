from angular_distance_reverse import angular_distance_reverse
import math

def define_limit(ra,dec,fov_sizex,fov_sizey,index_n):
    # function to calculate the egde of the FoV given 2 coordinates ie 4 arguments
    # return a list of 20 coordinates between the two points

    dec_high = dec + ( fov_sizey / 2. )
    if dec_high < -90:
        dec_high = -90
    elif dec_high > 90:
        dec_high = 90
    dec_low = dec - ( fov_sizey / 2. )
    if dec_low < -90:
        dec_low = -90
    elif dec_low > 90:
        dec_low = 90

    ind = 3.1415926 / 180.0

    dec_radian = abs(dec ) * ind
    ra_interval = fov_sizex / math.cos(dec_radian)
    dec_high_radian = abs(dec_high)  * ind
    ra_high_interval = fov_sizex / math.cos(dec_high_radian)
    dec_low_radian = abs(dec_low)  * ind
    ra_low_interval = fov_sizex / math.cos(dec_low_radian)

    ra_high1 = ra - (ra_high_interval/2.)
    if ra_high1 < 0:
        ra_high1 = 360. + ra_high1
    ra_high2 = ra + (ra_high_interval/2.)
    if ra_high2 > 360:
        ra_high2 = ra_high2 - 360.
    ra_low1 = ra - (ra_low_interval/2.)
    if ra_low1 < 0:
        ra_low1 = 360. + ra_low1
    ra_low2 = ra + (ra_low_interval/2.)
    if ra_low2 > 360:
        ra_low2 = ra_low2 - 360.

    ra_limit=[]
    dec_limit=[]
    n = index_n
    # Upper side
    for j in range(n):
        dec_i = dec_high
        ra_int = ra_high_interval / 2 / n 
        ra_i_1 = (ra-j*ra_int)
        ra_i_2 = (ra+j*ra_int)

        if ra_i_1 < 0:
                ra_i_1 = 360. + ra_i_1
        if ra_i_1 > 360:
            ra_i_1 = ra_i_1 - 360.        

        if ra_i_2 < 0:
                ra_i_2 = 360. + ra_i_2
        if ra_i_2 > 360:
            ra_i_2 = ra_i_2 - 360.

        ra_limit.append(ra_i_1)
        ra_limit.append(ra_i_2)
        dec_limit.append(dec_i) 
        dec_limit.append(dec_i)    

    # Lower side
    for j in range(n):
        dec_i = dec_low
        ra_int = ra_low_interval / 2 / n 
        ra_i_1 = (ra-j*ra_int)
        ra_i_2 = (ra+j*ra_int)

        if ra_i_1 < 0:
                ra_i_1 = 360. + ra_i_1
        if ra_i_1 > 360:
            ra_i_1 = ra_i_1 - 360.        

        if ra_i_2 < 0:
                ra_i_2 = 360. + ra_i_2
        if ra_i_2 > 360:
            ra_i_2 = ra_i_2 - 360.

        ra_limit.append(ra_i_1)
        ra_limit.append(ra_i_2)
        dec_limit.append(dec_i) 
        dec_limit.append(dec_i) 

    # Left and right side
    for j in range(n):
        dec_int = fov_sizey / 2 / n 
        dec_i_1 = dec - ( j * dec_int )
        if dec_i_1 < -90:
            dec_i_1 = -90
        elif dec_i_1 > 90:
            dec_i_1 = 90
        dec_i_2 = dec + ( j * dec_int )
        if dec_i_2 < -90:
            dec_i_2 = -90
        elif dec_i_2 > 90:
            dec_i_2 = 90

        ra_i_1_array = angular_distance_reverse(ra,dec_i_1,dec_i_1,(fov_sizey / 2))
        ra_i_2_array = angular_distance_reverse(ra,dec_i_2,dec_i_2,(fov_sizey / 2))

        ra_limit.append(ra_i_1_array[0])
        dec_limit.append(dec_i_1) 
        ra_limit.append(ra_i_1_array[1])
        dec_limit.append(dec_i_1) 
        ra_limit.append(ra_i_2_array[0])
        dec_limit.append(dec_i_2) 
        ra_limit.append(ra_i_2_array[1])
        dec_limit.append(dec_i_2) 
       
    return (ra_limit,dec_limit)
