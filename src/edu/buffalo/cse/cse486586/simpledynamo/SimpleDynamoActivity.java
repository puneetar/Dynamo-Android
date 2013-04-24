package edu.buffalo.cse.cse486586.simpledynamo;


import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.database.Cursor;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

		TextView tv = (TextView) findViewById(R.id.textView1);
		tv.setMovementMethod(new ScrollingMovementMethod());

		findViewById(R.id.button1).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) {
						new AsyncTask<Void, String, Void>(){

							@Override
							protected Void doInBackground(Void... params) {
								
								for(int key=0;key<20;key++){
									String sKey=String.valueOf(key);
									Log.v("SEND", "sending : "+sKey+ " to : "+SimpleDynamoProvider.getPortToSend(sKey));
									SimpleDynamoProvider.sendMessageAndACK(SimpleDynamoProvider.getPortToSend(sKey), Message.getMsgInsert(sKey, "Put1"+sKey));
									try {
										Thread.currentThread().sleep(1000);
									} catch (InterruptedException e) {
										Log.v("ERROR", "Thread has been interupted");
										e.printStackTrace();
									}
								}
								return null;
							}

							@Override
							protected void onProgressUpdate(String... strings ){
								super.onProgressUpdate(strings[0]);
							}
						}.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
					}
				});
		
		findViewById(R.id.button2).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) {
						new AsyncTask<Void, String, Void>(){

							@Override
							protected Void doInBackground(Void... params) {
								for(int key=0;key<20;key++){
									String sKey=String.valueOf(key);
									Log.v("SEND", "sending : "+sKey+ " to : "+SimpleDynamoProvider.getPortToSend(sKey));
									SimpleDynamoProvider.sendMessageAndACK(SimpleDynamoProvider.getPortToSend(sKey), Message.getMsgInsert(sKey, "Put2"+sKey));
									try {
										Thread.currentThread().sleep(1000);
									} catch (InterruptedException e) {
										Log.v("ERROR", "Thread has been interupted");
										e.printStackTrace();
									}
								}
								return null;
							}

							@Override
							protected void onProgressUpdate(String... strings ){
								super.onProgressUpdate(strings[0]);
							}
						}.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
					}
				});
		
		findViewById(R.id.button3).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) {
						new AsyncTask<Void, String, Void>(){

							@Override
							protected Void doInBackground(Void... params) {
								for(int key=0;key<20;key++){
									String sKey=String.valueOf(key);
									Log.v("SEND", "sending : "+sKey+ " to : "+SimpleDynamoProvider.getPortToSend(sKey));
									SimpleDynamoProvider.sendMessageAndACK(SimpleDynamoProvider.getPortToSend(sKey), Message.getMsgInsert(sKey, "Put3"+sKey));
									try {
										Thread.currentThread().sleep(1000);
									} catch (InterruptedException e) {
										Log.v("ERROR", "Thread has been interupted");
										e.printStackTrace();
									}
								}
								return null;
							}

							@Override
							protected void onProgressUpdate(String... strings ){
								super.onProgressUpdate(strings[0]);
							}
						}.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
					}
				});

		findViewById(R.id.button4).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) {
						new AsyncTask<Void, String, Void>(){

							@Override
							protected Void doInBackground(Void... params) {

								Cursor resultCursor = SimpleDynamoProvider.myContentResolver.query(SimpleDynamoProvider.myUri, null,"$GetAll$", null, null);
								while(resultCursor.moveToNext()){

									int keyIndex = resultCursor.getColumnIndex(SimpleDynamoProvider.KEY_FIELD);
									int valueIndex = resultCursor.getColumnIndex(SimpleDynamoProvider.VALUE_FIELD);

									String key = resultCursor.getString(keyIndex);
									String value= resultCursor.getString(valueIndex);

									//	System.out.println("PRINTING: "+"< "+key+" : "+value+" >");
									publishProgress("< "+key+" : "+value+" >\n");	
								}
								return null;
							}

							protected void onProgressUpdate(String... strings ){
								super.onProgressUpdate(strings[0]);
								TextView tv1 = (TextView) findViewById(R.id.textView1);
								tv1.append(strings[0]);
							}
						}.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
					}
				});
		
		findViewById(R.id.button5).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) {
						new AsyncTask<Void, String, Void>(){

							@Override
							protected Void doInBackground(Void... params) {
								for(int key=0;key<20;key++){
									String sKey=String.valueOf(key);
									Log.v("QUERY", "querying : "+sKey+ " to : "+SimpleDynamoProvider.getPortToSend(sKey));
									Message m=SimpleDynamoProvider.sendGETMessageAndACK(SimpleDynamoProvider.getPortToSend(sKey), Message.getMsgQuery(sKey));
									publishProgress("< "+m.getKey()+" : "+m.getValue()+" >\n");
									try {
										Thread.currentThread().sleep(1000);
									} catch (InterruptedException e) {
										Log.v("ERROR", "Thread has been interupted");
										e.printStackTrace();
									}
								}
								return null;
							}

							@Override
							protected void onProgressUpdate(String... strings ){
								super.onProgressUpdate(strings[0]);
								TextView tv1 = (TextView) findViewById(R.id.textView1);
								tv1.append(strings[0]);
							}
						}.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
					}
				});

	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}

	@Override
	protected void onDestroy (){
		//this.deleteDatabase(Database.DATABASE_NAME);
		//SimpleDhtProvider.context.deleteDatabase(Database.DATABASE_NAME);public void onDestroy() {   
	    super.onDestroy();
	    //eventsData.close();     
	
	}

}
