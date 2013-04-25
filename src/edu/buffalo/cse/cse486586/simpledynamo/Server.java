package edu.buffalo.cse.cse486586.simpledynamo;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;

public class Server extends AsyncTask<ServerSocket, String, Void> {

	@Override
	protected Void doInBackground(ServerSocket... sockets) {
		ServerSocket serverSocket = sockets[0];
		Socket socket=null;
		try{
		//	Log.v("STARTING SERVER", "123");
			while(true){
				socket = serverSocket.accept();
				//Node n=new Node(socket, new ObjectOutputStream(socket.getOutputStream()), new ObjectInputStream(socket.getInputStream()));
				new ServerImpl().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,socket);
			}
		}
		catch(IOException e){
//			Log.v("ERROR", "In server creation");
//			e.printStackTrace();
		}
		return null;
	}
}


class ServerImpl extends AsyncTask<Socket, String, Void> {

	private Socket socket;
	private ObjectOutputStream oostream;
	private ObjectInputStream oistream;
	private Message message;
	static int count;

	@Override
	protected Void doInBackground(Socket... sock) {
		this.socket=sock[0];
		try {
			oostream=new ObjectOutputStream(socket.getOutputStream());
			oistream=new ObjectInputStream(socket.getInputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		}
		

		try {
			message = (Message) oistream.readObject();
			switch(message.getType())
			{
			case Message.MSG_INSERT :
			case Message.MSG_FORCE_INSERT:
			case Message.MSG_REPLICA:
			//	System.out.println("In insert Message");
				insert(message);
				oostream.writeObject(Message.getMsgAck());	
				socket.close();
				break;
			case Message.MSG_QUERY:
			//	System.out.println("In query message");
				query(message);
				socket.close();
				break;
			case Message.MSG_RECOVERED:
			//	System.out.println("In Recovered message");
				recovered();
				socket.close();
				break;
			default:
				socket.close();
				break;

			}
			//			if (message.getType() == Message.MSG_INSERT) {
			//				System.out.println("In insert Message");
			//				insert(message);
			//				oostream.writeObject(Message.getMsgAck());	
			//				socket.close();
			//
			//			} else if (message.getType() == Message.MSG_FORCE_INSERT) {
			//				System.out.println("In force_insert message");
			//				insert(message);
			//				oostream.writeObject(Message.getMsgAck());
			//				socket.close();
			//
			//			}  else if (message.getType() == Message.MSG_REPLICA) {
			//				System.out.println("In replica message");
			//				insert(message);
			//				oostream.writeObject(Message.getMsgAck());
			//				socket.close();
			//
			//			}else if (message.getType() == Message.MSG_L_DUMP) {
			//				//	System.out.println("In insert message");
			//				socket.close();
			//
			//			} else if (message.getType() == Message.MSG_QUERY) {
			//				System.out.println("In query message");
			//				query(message);
			//				socket.close();
			//
			//			} else if (message.getType() == Message.MSG_QUERY_REPLY) {
			//				System.out.println("In Query_Reply message");
			//				socket.close();
			//
			//			} else if (message.getType() == Message.MSG_ACK) {
			//				System.out.println("In ACK message");
			//				socket.close();
			//
			//			} else if (message.getType() == Message.MSG_RECOVERED) {
			//				System.out.println("In Recovered message");
			//				recovered();
			//				socket.close();
			//
			//			} else {
			//				System.out.println(" message type cannot be found ***");
			//				socket.close();
			//			}

		} catch (OptionalDataException e) {
			e.printStackTrace();
			System.out.println("SERVER IMPL class : error in read object");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("SERVER IMPL class : error in read object");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("SERVER IMPL class : error in read object");
		}

		return null;
	}

	private void recovered() {
		Cursor cursor = SimpleDynamoProvider.myContentResolver.query(SimpleDynamoProvider.myUri, null, "$GetAll$", null, null);
		ArrayList<Message> arr=new ArrayList<Message>();

		while(cursor.moveToNext()){

			int keyIndex = cursor.getColumnIndex(SimpleDynamoProvider.KEY_FIELD);
			int valueIndex = cursor.getColumnIndex(SimpleDynamoProvider.VALUE_FIELD);

			String key = cursor.getString(keyIndex);
			String value= cursor.getString(valueIndex);

			arr.add(Message.getMsgInsert(key, value));
		}

		try {
			oostream.writeObject(arr);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}



	private void query(Message m) {

		Cursor resultCursor = SimpleDynamoProvider.myContentResolver.query(SimpleDynamoProvider.myUri, null, m.getKey(), null, null);
		try {
			if(resultCursor.moveToFirst()){
				//	System.out.println("yes i have a row");
			}
			oostream.writeObject(Message.getMsgQueryReply(resultCursor.getString(0), resultCursor.getString(1)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void insert(Message m) {
		ContentValues cv = new ContentValues();
		cv.put(SimpleDynamoProvider.KEY_FIELD, message.getKey());
		cv.put(SimpleDynamoProvider.VALUE_FIELD, message.getValue());
		//System.out.println("**in method insert ");
		SimpleDynamoProvider.myContentResolver.insert(SimpleDynamoProvider.myUri, cv);
	}



	private void sendMessage(int port,Message m) {

		try {
			Socket socket=new Socket(SimpleDynamoProvider.IP, port);
			//System.out.println("send message");
			Node n=new Node(socket, new ObjectOutputStream(socket.getOutputStream()), new ObjectInputStream(socket.getInputStream()));

			n.getOostream().writeObject(m);		
			socket.close();

		} catch (StreamCorruptedException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Message sendGetMessage(int port,Message m) {

		Message ret=null;
		try {
			Socket socket=new Socket(SimpleDynamoProvider.IP, port);
			//System.out.println("send message: "+port);
			Node n=new Node(socket, new ObjectOutputStream(socket.getOutputStream()), new ObjectInputStream(socket.getInputStream()));

			n.getOostream().writeObject(m);	
			ret=(Message)n.getOistream().readObject();
			socket.close();

		} catch (StreamCorruptedException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return ret;
	}

}
