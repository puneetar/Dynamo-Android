package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;


public class Message implements Serializable{

	public static final int MSG_INSERT=1;
	public static final int MSG_FORCE_INSERT=2;
	public static final int MSG_REPLICA=3;

	public static final int MSG_QUERY=4;
	public static final int MSG_QUERY_REPLY=5;
	
	public static final int MSG_L_DUMP=6;
	public static final int MSG_ACK=7;
	public static final int MSG_RECOVERED=8;
	
	public static String FORCE="$Force$";
	public static String REPLICA="$Replica$";
	
	private int type;
	private String value;
	private String key;
	
	private Message(int t,String key, String val){
		this.type=t;
		this.key=key;
		this.value=val;
	}
	
	private Message(int t){
		this(t,null,null);
	}
	
	public static Message getMsgInsert(String key,String val) {
		return new Message(MSG_INSERT, key,val);
	}
	
	public static Message getMsgForceInsert(String key,String val) {
		return new Message(MSG_FORCE_INSERT, FORCE+key,val);
	}	
	
	public static Message getMsgForceInsert(Message m) {
		return new Message(MSG_FORCE_INSERT, FORCE+m.getKey(), m.getValue());
	}

	public static Message getMsgReplica(String key,String val) {
		return new Message(MSG_REPLICA, REPLICA+key,val);
	}

	public static Message getMsgReplica(Message m) {
		return new Message(MSG_REPLICA, REPLICA+m.getKey(),m.getValue());
	}

	public static Message getMsgQuery(String key) {
		return new Message(MSG_QUERY, key,null);
	}
	
	public static Message getMsgQueryReply(String key,String val){
		return new Message(MSG_QUERY_REPLY, key, val);
	}
	
	public static Message getMsgAck(){
		return new Message(MSG_ACK);
	}
	
	public static Message getMsgRecovered(){
		return new Message(MSG_RECOVERED);
	}
	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
