import sqlite3

database = 'tenant_test.db'
tablename='tenantDBtable1'
conn = sqlite3.connect('tenant_new.db')
#conn = sqlite3.connect(database)
c =conn.cursor()


def createTable():
  c.execute("CREATE TABLE IF NOT EXISTS %s (tenant_mac TEXT,vni INT,tun_port INT, tun_dst TEXT,dpid TEXT)"%(tablename))


def insert():
  tenant_mac=input("Enter Tenants MAC address\n")
  dpid = input("Enter DPID of the switch to which this tenant is connected\n")
  vni = input("Enter VNI assigned to the tenant\n")
  vni = int(vni)
  out_port = input("Enter Virtual tunnel port associated with the tenants switch\n") 
  out_port = int(out_port)
  tun_dst = input("Enter Remote VTEP ips which belongs to this VNI\n")
  c.execute("INSERT INTO %s (tenant_mac,vni,tun_port,tun_dst,dpid) VALUES (?,?,?,?,?)"%tablename,(tenant_mac,vni,out_port,tun_dst,dpid))
  conn.commit()

def select():
  sql="SELECT * from %s"%tablename
  #c.execute("header on")
  #c.execute(".mode column ")
  for row in c.execute(sql):
        tmac, vni, tun_port,dst,dpid= row
        print "DPID : %s vni : %d tun_port: %d tun_dst: %s " %(dpid,vni,tun_port,dst)


if __name__ == '__main__':
  print "Select operation\n"
   
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

