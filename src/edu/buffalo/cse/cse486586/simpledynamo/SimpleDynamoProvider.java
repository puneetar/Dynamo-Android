package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	int port;
	public static String IP ="10.0.2.2";
	public static String TELE=null,HASH=null;
	public static int PORT=0;
	public static String HASH_5554=null,HASH_5556=null,HASH_5558=null;

	public static ContentResolver myContentResolver;
	public static Uri myUri;

	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";

	public static Context context;

	SQLiteDatabase sql_db;
	public static Database data_base;

	@Override
	public boolean onCreate() {
		getContext().deleteDatabase(Database.DATABASE_NAME);

		System.out.println("Content provider is called");
		context=getContext();
		System.out.println("in onCreate : going to create database");
		data_base = new Database(context);
		data_base.getReadableDatabase();

		myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		myContentResolver = context.getContentResolver();


		TelephonyManager tel =(TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		TELE = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		PORT=getPort(TELE);

		try {
			HASH=genHash(TELE);
			HASH_5554=genHash("5554");
			HASH_5556=genHash("5556");
			HASH_5558=genHash("5558");
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			Log.v("ERROR", "In Generating HASH");
			e1.printStackTrace();
		}

		try{
			ServerSocket serverSocket = new ServerSocket (10000);
			new Server().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR , serverSocket);
		}catch(Exception e){
			Log.v("ERROR", "In server creation");
		}


		new AsyncTask<Void, Void, Void>(){

			@Override
			protected Void doInBackground(Void... params) {
				Object obj=null;
				for(int i=0;i<3;i++){
					obj=sendGetMessage(getSuccPort(TELE), Message.getMsgRecovered());
					if(obj!=null){
						ArrayList<Message> arrMsg = (ArrayList<Message>)obj;
						Iterator<Message> it=arrMsg.iterator();
						while(it.hasNext()){
							Message msg=it.next();
							ContentValues cv = new ContentValues();
							cv.put(SimpleDynamoProvider.KEY_FIELD, msg.getKey());
							cv.put(SimpleDynamoProvider.VALUE_FIELD, msg.getValue());
							sql_db=data_base.getWritableDatabase();
							sql_db.replace(Database.TABLE_NAME, null, cv);
							myContentResolver.notifyChange(myUri, null);
						}
						break;
					}	
				}
				return null;
			}
		}.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);


		return true;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key=values.getAsString(Database.KEY);
		String val=values.getAsString(Database.VALUE);

		if(key.startsWith(Message.FORCE)){

			ContentValues cv = new ContentValues();
			cv.put(SimpleDynamoProvider.KEY_FIELD, key.substring(Message.FORCE.length()));
			cv.put(SimpleDynamoProvider.VALUE_FIELD, val);

			//System.out.println("inserting in my database");
			sql_db=data_base.getWritableDatabase();
			sql_db.replace(Database.TABLE_NAME, null, cv);
			myContentResolver.notifyChange(uri, null);

		}else if(key.startsWith(Message.REPLICA)){

			ContentValues cv = new ContentValues();
			cv.put(SimpleDynamoProvider.KEY_FIELD, key.substring(Message.REPLICA.length()));
			cv.put(SimpleDynamoProvider.VALUE_FIELD, val);

			//System.out.println("inserting in my database");
			sql_db=data_base.getWritableDatabase();
			sql_db.replace(Database.TABLE_NAME, null, cv);
			myContentResolver.notifyChange(uri, null);

		}else{

			sql_db=data_base.getWritableDatabase();
			sql_db.replace(Database.TABLE_NAME, null, values);
			myContentResolver.notifyChange(uri, null);

			//send REPLICA to the next 2 successors
			new AsyncTask<String, Void, Void>(){

				@Override
				protected Void doInBackground(String... params) {
					Message m=Message.getMsgReplica(params[0], params[1]);
					sendMessageAndACK(getSuccPort(TELE), m);
					sendMessageAndACK(getSuccPort(getTele(getSuccPort(TELE))), m);
					return null;
				}

			}.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, new String[]{key,val});

		}

		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		
		if(selection.equalsIgnoreCase("$GetAll$")){
			
			SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
			queryBuilder.setTables(Database.TABLE_NAME);
			sql_db = data_base.getReadableDatabase();
			Cursor cursor = null;
			cursor = queryBuilder.query(sql_db, projection, null,null, null, null, sortOrder);

			cursor.setNotificationUri(myContentResolver, uri);
			return cursor;

		}else{
			SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
			queryBuilder.setTables(Database.TABLE_NAME);
			sql_db = data_base.getReadableDatabase();
			Cursor cursor = null;
			cursor = queryBuilder.query(sql_db, projection, Database.KEY + "=?", new String[] {selection}, null, null, sortOrder);

			cursor.setNotificationUri(myContentResolver, uri);
			return cursor;
			
		}
	}


	private boolean sendMessage(int port,Message m) {

		try {
			Socket socket=new Socket(IP, port);
			Node n=new Node(socket, new ObjectOutputStream(socket.getOutputStream()), new ObjectInputStream(socket.getInputStream()));
			n.getOostream().writeObject(m);		
			socket.close();
		} catch (StreamCorruptedException e) {
			Log.v("ERROR", "Exception ");
			e.printStackTrace();
			return false;
		} catch (UnknownHostException e) {
			Log.v("ERROR", "Exception ");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			Log.v("ERROR", "Exception ");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static boolean sendMessageAndACK(int port,Message m) {

		while(true){
			Message ret=null;
			try {
				Socket socket=new Socket(IP, port);
				socket.setSoTimeout(900);
				Node n=new Node(socket, new ObjectOutputStream(socket.getOutputStream()), new ObjectInputStream(socket.getInputStream()));
				n.getOostream().writeObject(m);	
				socket.setSoTimeout(900);
				ret=(Message)n.getOistream().readObject();
				if(ret.getType()!=Message.MSG_ACK)
					throw new Exception();

				socket.close();
				return true;
			} catch (Exception e) {
				Log.v("ERROR", "Exception in getting ACK from : "+port);
				port=getSuccPort(getTele(port));
				Log.v("DEBUG", "Now sending to : "+port);
				e.printStackTrace();
				//return false;
			}
		}
	}
	
	public static Message sendGETMessageAndACK(int port,Message m) {

		while(true){
			Message ret=null;
			try {
				Socket socket=new Socket(IP, port);
				socket.setSoTimeout(1000);
				Node n=new Node(socket, new ObjectOutputStream(socket.getOutputStream()), new ObjectInputStream(socket.getInputStream()));
				n.getOostream().writeObject(m);	
				socket.setSoTimeout(1000);
				ret=(Message)n.getOistream().readObject();
				socket.close();
				return ret;
			} catch (Exception e) {
				Log.v("ERROR", "Exception in getting GET_ACK from : "+port);
				port=getSuccPort(getTele(port));
				Log.v("DEBUG", "Now sending to GET: "+port);
				e.printStackTrace();
				//return false;
			}
		}
	}

	private Object sendGetMessage(int port,Message m) {

		Object ret=null;
		try {
			Socket socket=new Socket(IP, port);
			Node n=new Node(socket, new ObjectOutputStream(socket.getOutputStream()), new ObjectInputStream(socket.getInputStream()));
			n.getOostream().writeObject(m);	
			ret=n.getOistream().readObject();
			socket.close();
		} catch (StreamCorruptedException e) {
			Log.v("ERROR", "Exception ");
			e.printStackTrace();
		} catch (UnknownHostException e) {
			Log.v("ERROR", "Exception ");
			e.printStackTrace();
		} catch (IOException e) {
			Log.v("ERROR", "Exception ");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			Log.v("ERROR", "Exception ");
			e.printStackTrace();
		}

		return ret;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public static int getPortToSend(String key){
		String HASH_KEY = null;

		try {
			HASH_KEY = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			Log.v("ERROR", "In generation of HASH");
			e.printStackTrace();
		}

		if(HASH_KEY.compareTo(HASH_5556)<=0)
			return getPort("5556");
		else if(HASH_KEY.compareTo(HASH_5554)<=0)
			return getPort("5554");
		else if(HASH_KEY.compareTo(HASH_5558)<=0)
			return getPort("5558");
		else 
			return getPort("5556");
	}

	public static int getSuccPort(String tele){
		if(tele.equalsIgnoreCase("5554"))
			return getPort("5558");
		else if(tele.equalsIgnoreCase("5556"))
			return getPort("5554");
		else if(tele.equalsIgnoreCase("5558"))
			return getPort("5556");
		else{
			System.out.println("UNABLE TO GET SUCCESSOR PORT");
			return 0;
		}
	}

	public static int getPort(String tele){
		if(tele.equalsIgnoreCase("5554"))
			return 11108;
		else if(tele.equalsIgnoreCase("5556"))
			return 11112;
		else if(tele.equalsIgnoreCase("5558"))
			return 11116;
		else{
			System.out.println("UNABLE TO GET PORT");
			return 0;
		}	
	}

	public static String getTele(int port){
		if(port==11108)
			return "5554";
		else if(port==11112)
			return "5556";
		else if(port==11116)
			return "5558";
		else{
			System.out.println("UNABLE TO GET TELE");
			return "0";
		}	
	}

	private static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

}
