"""

   Pubsub envelope subscriber   
 
   Author: Xuhui Han
  
"""

import json
import pymysql

#__________________________________
def CMM_DBConnect(location):
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open("db_param.json", "r") as read_file:
        data = json.load(read_file)
    if location == 'beijing':
        CMM_user = data["CMM_user_bj"]
    elif location == 'xinglong':
        CMM_user = data["CMM_user_xl"]
    # db = pymysql.connect(data["CMM_host"],CMM_user,data["CMM_password"],data["CMM_db"] )
    try:
        db = pymysql.connect(data["CMM_host"],CMM_user,data["CMM_password"],data["CMM_db"] )
    except:
        print('can not connect to the CMM DB')
        db = []
    return db

#__________________________________
def CMM_DBClose(db):
    "Close the connection to the DB"
    db.close()

#__________________________________
def get_trigger_name_by_id(location,id):
    "Find the trigger ID"
    db = CMM_DBConnect(location)
    # print(db)
    if db != []:
        try:
            cursor = db.cursor()
            query = "SELECT external_trigger_name FROM trigger_v where external_trigger_type_simulation=1 and ID_external_trigger=%s" %str(id)
            cursor.execute(query)
            results = cursor.fetchall()
            # print(results)
            if results:
                ID = results[0][0]
            else:
                ID = 0
            cursor.close()
            CMM_DBClose(db)
        except:
            print("1. lost connection to Mysql server during the query")
            ID = 0
    else:
        ID = 0
    return ID

if __name__ == '__main__':
    pass