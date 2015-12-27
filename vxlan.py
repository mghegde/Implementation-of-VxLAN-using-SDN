
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_4
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
import sqlite3

class vxlan(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(vxlan, self).__init__(*args, **kwargs)
        self.mac_to_port = {}

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]

        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return
        dst = eth.dst
        src = eth.src

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

        sql="SELECT vni,out_port,tun_dst,dpid from tenantDBtable1 where tenant_mac=?"

        sql1="SELECT * from dpidtable"
	port=-1
	vni_no=0
	dst_tun_ip=""

	conn = sqlite3.connect('tenant_new.db')
	c =conn.cursor()
        
	conn1 = sqlite3.connect('dpid.db')
	c1 =conn1.cursor()

        for row in c.execute(sql,[(src)]):
           vni_no,port,dst_tun_ips,switch_dpid= row

  	   dst_tun_ip=dst_tun_ips.split(',')

	
	dpid_to_tun={}
	dpid_to_port={}
        #for row in c1.execute(sql1):
        #   sw_dp,l_tun= row
	#   dpid_to_tun[sw_dp]=l_tun
	#   print sw_dp,l_tun
	   
        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port
	print ""
	print '_'*20
	self.logger.info("At Switch: %s. Learnt mac_table[%s]=%s", dpid, src, in_port)
	print '_'*20
	print ""
        if dst in self.mac_to_port[dpid]:
	    #Find outgoig interface
            out_port = self.mac_to_port[dpid][dst]
	    #To multiple interfaces connnected through that output interface for this VNI
	    if len(dst_tun_ip) > 1:
		#Extract Remote tunnel IP from the database
                for row in c1.execute(sql1):
           	  sw_dp,l_tun,sw_port= row
	   	  dpid_to_tun[sw_dp]=l_tun
	   	  dpid_to_port[sw_dp]=sw_port

		for dp in self.mac_to_port:
		  if dst in self.mac_to_port[dp]:
		     #Find Remote tunnel IP from dpid_to_tun dict
		     if dp != dpid :
		       #Make sure host present in the mac_port map is directly connected 
		       if self.mac_to_port[dp][dst] != dpid_to_port[str(dp)]:
		       	 dst_tun_ip=dpid_to_tun[str(dp)]
		       	 break
	    else :
		#If len is zero May be local subscriber in the LAN. SO entry not present
		if len(dst_tun_ip) !=0:
		  dst_tun_ip=dst_tun_ip[0]	   	
        else:
	    #MAC address not yet learned. So flood the packet
	    out_port = ofproto.OFPP_FLOOD
	    #if len(dst_tun_ip) ==1 :
	    #  dst_tun_ip=dst_tun_ip[0]

         
        if vni_no !=0 and (out_port == ofproto.OFPP_FLOOD or out_port == port) :
	  # actions =[parser.OFPActionSetField(tunnel_id=vni_no),parser.OFPActionSetField(tun_ipv4_dst=dst_tun_ip), parser.OFPActionOutput(out_port)]
	   if (type(dst_tun_ip) is list) and len(dst_tun_ip) > 1:
	      count =0
	      for d_ip in dst_tun_ip:
		 actions =[parser.OFPActionSetField(tunnel_id=vni_no),parser.OFPActionSetField(tun_ipv4_dst=d_ip), parser.OFPActionOutput(out_port)]
        	 data = None
		 count+=1
		
  		 #Extra packets which we sendout are not buffered in OVS. So set NO_BUF flag and copy the message before sending
		 # Otherwise if the message is empty, OVS ignores the send_msg
		 if count >1:
		   buf = ofproto.OFP_NO_BUFFER
		 else :
		   buf = msg.buffer_id

        	 if buf == ofproto.OFP_NO_BUFFER:
            	    data = msg.data

        	 out = parser.OFPPacketOut(datapath=datapath, buffer_id=buf,
                                         in_port=in_port, actions=actions, data=data)
        	 datapath.send_msg(out)

	      return

           else:
	     if type(dst_tun_ip) is list :
		dst_tun_ip=dst_tun_ip[0]
	     actions =[parser.OFPActionSetField(tunnel_id=vni_no),parser.OFPActionSetField(tun_ipv4_dst=dst_tun_ip), parser.OFPActionOutput(out_port)]

	else :
	   actions =[parser.OFPActionOutput(out_port)]

        #actions = [parser.OFPActionOutput(out_port)]

	#actions.append(datapath.ofproto_parser.OFPActionSetField(tunnel_id=100))
        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)
            self.add_flow(datapath, 1, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
	
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
