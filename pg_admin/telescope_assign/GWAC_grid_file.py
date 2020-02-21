
def GWAC_grid_file(grid_id):

    if grid_id == "G0007":
      grid_input = './grid_file/grid_0007.dat'
    elif grid_id == "G0008":
      grid_input = './grid_file/grid_0008.dat'
    elif grid_id == "G0010":
      grid_input = './grid_file/grid_0010.dat'
    elif grid_id == "G0012":
      grid_input = './grid_file/grid_0012.dat'
    elif grid_id == "G0013":
      grid_input = './grid_file/grid_0013.dat'
    elif grid_id == "G0014":
      grid_input = './grid_file/grid_0014.dat'
    elif grid_id == "G0015":
      grid_input = './grid_file/grid_0015.dat'
    elif grid_id == "G0016":
      grid_input = './grid_file/grid_0016.dat'
    else:
      print("invalid grid_id")

    return grid_input