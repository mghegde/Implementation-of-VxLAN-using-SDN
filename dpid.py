import sqlite3

database='dpid.db'
#database='dpid_test.db'
tablename='dpidtable'
conn = sqlite3.connect(database)
c =conn.cursor()

dpid="3"
tun_src="10.0.3.2"
sw_port=10



def createTable():
  c.execute("CREATE TABLE IF NOT EXISTS %s (dpid TEXT,tun_src TEXT,sw_port INT)"%tablename)


def insert():
  dpid = input("Enter DPID of the switch to which this tenant is connected\n")
  tun_src = input("Enter Source address of the tunnel\n")
  sw_port = input("Enter Virtual tunnel port number\n")
  sw_port=int(sw_port)
  c.execute("INSERT INTO %s (dpid,tun_src,sw_port) VALUES (?,?,?)"%tablename,(dpid,tun_src,sw_port))
  conn.commit()

def select():
  sql="SELECT * from %s"%tablename
  for row in c.execute(sql):
        dpid,tun_src,sw_port= row
        print "DPID : %s tun_src : %s sw_port: %d " %(dpid,tun_src,sw_port)


if __name__ == '__main__':
  print "Select operation"

  while 1 :
    val=input("\n1. Create Table  2. INSERT table 3. Display 4.Exit\n")

    if val ==1 :
       createTable()
       print "Table created"
    elif val == 2:
       insert()
    elif val ==3 :
       print '_'*20
       print ""
       select()
       print ""
       print '_'*20
    elif val == 4 :
       break

