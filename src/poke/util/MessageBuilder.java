package poke.util;

import poke.comm.Image.Header;
import poke.comm.Image.PayLoad;
import poke.comm.Image.Ping;
import poke.comm.Image.Request;
import poke.core.Mgmt.DataSet;
import poke.core.Mgmt.LogEntry;
import poke.core.Mgmt.LogEntryList;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.NameValueSet;
import poke.core.Mgmt.RaftMessage;
import poke.resources.data.MgmtResponse;

import com.google.protobuf.ByteString;

public class MessageBuilder {
	
	public static  Request buildPingMessage(){
		
		Request.Builder r = Request.newBuilder();

		PayLoad.Builder p = PayLoad.newBuilder();
		p.setData(null);

		r.setPayload(p.build());

		// header with routing info
		Header.Builder h = Header.newBuilder();
		h.setClientId(-1);
		h.setClusterId(3);
		h.setCaption("Ping Message");
		h.setIsClient(false);

		r.setHeader(h.build());

		Ping.Builder pg = Ping.newBuilder();
		pg.setIsPing(true);

		r.setPing(pg.build());

		return r.build();
		
	}
	public static Management buildMgmtMessage(Request request) {

		NameValueSet.Builder nameAndValue = NameValueSet.newBuilder();
		nameAndValue.setName(request.getHeader().getCaption());
		nameAndValue.setValue(request.getPayload().getReqId());

		DataSet.Builder dataSet = DataSet.newBuilder();
		dataSet.setKey(request.getPayload().getReqId());
		if (request.getHeader().getIsClient())
			dataSet.setClientId(request.getHeader().getClientId());
		else
			dataSet.setClientId(-1);
		dataSet.setDataSet(nameAndValue.build());

		LogEntry.Builder logEntry = LogEntry.newBuilder();
		logEntry.setAction(LogEntry.DataAction.INSERT);
		logEntry.setTerm(-1);
		logEntry.setData(dataSet.build());

		LogEntryList.Builder logList = LogEntryList.newBuilder();
		logList.addEntry(logEntry.build());

		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.CLIENTREQUEST);
		rmb.setEntries(logList.build());
		rmb.setTerm(-1);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(-1);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());

		return mb.build();

	}

	public static Request buildRequestMessage(MgmtResponse mgmt, ByteString bs) {

		Request.Builder r = Request.newBuilder();

		PayLoad.Builder p = PayLoad.newBuilder();
		p.setReqId(mgmt.getDataSet().getKey());
		p.setData(bs);

		r.setPayload(p.build());

		// header with routing info
		Header.Builder h = Header.newBuilder();
		h.setClientId(mgmt.getDataSet().getClientId());
		h.setCaption(mgmt.getDataSet().getDataSet().getName());
		h.setIsClient(false);

		r.setHeader(h.build());

		Ping.Builder pg = Ping.newBuilder();
		pg.setIsPing(false);

		r.setPing(pg.build());

		return r.build();

	}

}
