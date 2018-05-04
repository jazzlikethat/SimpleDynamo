package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	ArrayList<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList("11124", "11112", "11108", "11116", "11120"));
	static final int SERVER_PORT = 10000;
	String myPort;

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	Uri uri = new Uri.Builder().authority("edu.buffalo.cse.cse486586.simpledynamo.provider").scheme("content").build();

	Object object = new Object();

	boolean insertionInProgress = false;
	boolean globalDumpComplete  = false;

	Map<String, String> queryResponse = new HashMap<String, String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private int getStringDifference(String filename, String port){

		int difference = 0;
		String port_string = null;

		try {
			port_string = Integer.toString(Integer.parseInt(port) / 2);
			difference = genHash(filename).compareTo(genHash(port_string));
		}
		catch (Exception e) {
			Log.e(TAG, "GenHash failed: " + filename);
		}

		return difference;
	}

	private String getTargetInsertPort(String filename){

		String targetPort = null;
		String firstPort = REMOTE_PORTS.get(0);
		String lastPort = REMOTE_PORTS.get(REMOTE_PORTS.size() - 1);

		if (getStringDifference(filename, firstPort) <= 0 || getStringDifference(filename, lastPort) > 0) {
			targetPort = firstPort;
			return targetPort;
		}

		for (int i = 0; i < REMOTE_PORTS.size() - 1; i++){
			String curPort = REMOTE_PORTS.get(i);
			String nextPort = REMOTE_PORTS.get(i+1);
			if (getStringDifference(filename, curPort) > 0 && getStringDifference(filename, nextPort) <= 0){
				targetPort = nextPort;
				break;
			}
		}

		return targetPort;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String filename = values.getAsString(KEY_FIELD);
		String content =  values.getAsString(VALUE_FIELD) + "\n";

		String targetPort = getTargetInsertPort(filename);


		try {
			if (targetPort.equals(myPort)){ // save the key in the current avd
				FileOutputStream outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
				outputStream.write(content.getBytes());
				outputStream.close();

				targetPort = getNextPort(targetPort);
			}


			insertionInProgress = true;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "forwardInsert", myPort, targetPort, filename, content);
			synchronized (object){
				while (insertionInProgress){
					object.wait();
				}
			}
		}
		catch (Exception e) {
			Log.e(TAG, "File write / forward failed: " + filename);
		}

		return uri;
	}

	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portString = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portString) * 2));

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Unable to create a ServerSocket");
			return false;
		}

		return false;
	}

	public String getNextPort(String port){
		int index = REMOTE_PORTS.indexOf(port);
		String nextPort = null;

		if (index == REMOTE_PORTS.size() - 1){
			nextPort = REMOTE_PORTS.get(0);
		}
		else {
			nextPort = REMOTE_PORTS.get(index + 1);
		}

		return nextPort;
	}

	public String getPrevPort(String port){
		int index = REMOTE_PORTS.indexOf(port);
		String prevPort = null;

		if (index == 0){
			prevPort = REMOTE_PORTS.get(REMOTE_PORTS.size() - 1);
		}
		else {
			prevPort = REMOTE_PORTS.get(index - 1);
		}

		return prevPort;
	}

	public Void handleForwardInsert(String msgReceived){
		String msg_split[] = msgReceived.split("###");
		String target_port = getTargetInsertPort(msg_split[2]);
		String target_port_2 = null;
		String successor1 = getNextPort(target_port);
		String successor2 = getNextPort(successor1);

		if (myPort.equals(successor2)){
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertComplete", myPort, msg_split[1]);
		}
		else {
			if (myPort.equals(target_port)){
				target_port_2 = successor1;
			}
			else if (myPort.equals(successor1)){
				target_port_2 = successor2;
			}
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "forwardInsert", msg_split[1], target_port_2, msg_split[2], msg_split[3]);
		}

		return null;
	}

	public Void handleForwardQuery(String msgReceived){
		String msg_split[] = msgReceived.split("###");
		String selection = msg_split[2];

		boolean isSelectionPresent = false;

		try {
			String listOfFiles[] = getContext().fileList();
			for (String S : listOfFiles){
				if (S.equals(selection)){
					isSelectionPresent = true;
					break;
				}
			}

			if (isSelectionPresent){
				// return the file with value
				FileInputStream fileInputStream = getContext().openFileInput(selection);
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
				String content = bufferedReader.readLine();
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"queryResponse", myPort, msg_split[1], selection, content);
			}
			else {
				// Forward the query
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "forwardQuery", msg_split[1], getPrevPort(myPort), selection);
			}
		}
		catch (Exception e) {
			Log.d(TAG, "File name failed "+selection);
			Log.d(TAG, "Reading file failed "+ e.getLocalizedMessage());
		}

		return null;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			String msgReceived;

			while (true){
				try {
					Socket socket = serverSocket.accept();
					InputStream inputStream = socket.getInputStream();
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					msgReceived = bufferedReader.readLine();
					publishProgress(msgReceived);
					bufferedReader.close();
					socket.close();
				}
				catch (IOException e) {
					Log.e(TAG, "Failed to publish message");
				}
			}
		}

		protected void onProgressUpdate(String...strings) {
			String msgReceived = strings[0].trim();
			String msg_split[] = msgReceived.split("###");

			if (msg_split[0].equals("forwardInsert")){

				try {
					FileOutputStream outputStream = getContext().openFileOutput(msg_split[2], Context.MODE_PRIVATE);
					outputStream.write(msg_split[3].getBytes());
					outputStream.close();
				}
				catch (Exception e) {
					Log.e(TAG, "File write failed: " + msg_split[2]);
				}

				handleForwardInsert(msgReceived);
			}
			else if (msg_split[0].equals("insertComplete")){
				synchronized (object){
					insertionInProgress = false;
					object.notifyAll();
				}
			}
			else if (msg_split[0].equals("forwardQuery")){
				handleForwardQuery(msgReceived);
			}
			else if (msg_split[0].equals("queryResponse")){
				queryResponse.put(msg_split[2], msg_split[3]);
				synchronized (queryResponse)
				{
					queryResponse.notifyAll();
				}
			}
//			else if (msg_split[0].equals("globalQuery")){
//				handleGlobalQuery(msgReceived);
//			}
//			else if (msg_split[0].equals("globalQueryResponse")){
//				handleGlobalQueryResponse(msgReceived);
//			}
//			else if (msg_split[0].equals("deleteFile")){
//				handleDeleteFile(msgReceived);
//			}
//			else if (msg_split[0].equals("globalDelete")){
//				handleGlobalDelete(msgReceived);
//			}

			return;
		}
	}

			@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		try {
			MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
			FileInputStream fileInputStream;
			BufferedReader bufferedReader;
			String content;

			if (selection.equals("@")){
				String listOfFiles[] = getContext().fileList();
				for (String S : listOfFiles){
					fileInputStream = getContext().openFileInput(S);
					bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
					content = bufferedReader.readLine();
					cursor.addRow(new String[] {S, content});
				}
				return cursor;
			}
			else if (selection.equals("*")){
				String listOfFiles[] = getContext().fileList();
				for (String S : listOfFiles){
					fileInputStream = getContext().openFileInput(S);
					bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
					content = bufferedReader.readLine();
					cursor.addRow(new String[] {S, content});
				}
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"globalQuery", myPort, getNextPort(myPort));
				synchronized (queryResponse){
					while (!globalDumpComplete){
						queryResponse.wait();
					}
					globalDumpComplete = false;
					for (Map.Entry<String, String> entry : queryResponse.entrySet())
					{
						cursor.addRow(new String[] {entry.getKey(), entry.getValue()});
					}
				}
				return cursor;
			}
			else {
				boolean isSelectionPresent = false;
				String listOfFiles[] = getContext().fileList();
				for (String S : listOfFiles){
					if (S.equals(selection)){
						isSelectionPresent = true;
						break;
					}
				}

				if (isSelectionPresent){
					fileInputStream = getContext().openFileInput(selection);
					bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
					content = bufferedReader.readLine();
					cursor.addRow(new String[] {selection, content});
					return cursor;
				}

				String target_port = getTargetInsertPort(selection);
				String successor_2 = getNextPort(getNextPort(target_port));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "forwardQuery", myPort, successor_2, selection);
				synchronized (queryResponse){
					while (queryResponse.isEmpty()){
						queryResponse.wait();
					}
					content = queryResponse.get(selection);
					queryResponse.clear();
					cursor.addRow(new String[] {selection, content});
				}
				return cursor;
			}
		}
		catch (Exception e) {
			Log.e(TAG, "File name failed "+selection);
			Log.e(TAG, "Reading file failed "+ e.getLocalizedMessage());
		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			String REMOTE_PORT = null;
			
			try {
				REMOTE_PORT = msgs[2];
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(REMOTE_PORT));

				String msgToSend = "";

				for (int i = 0; i < msgs.length; i++){
					if (i == 2){
						continue;
					}

					msgToSend += msgs[i];

					if (i != msgs.length - 1){
						msgToSend += "###";
					}
				}

				OutputStream outputStream = socket.getOutputStream();
				PrintWriter printWriter = new PrintWriter(outputStream, true);
				printWriter.print(msgToSend);
				printWriter.flush();

				socket.close();
			}
			catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			}
			catch (IOException e) {
				e.printStackTrace();
				Log.e(TAG, "ClientTask socket IOException");
			}

			return null;
		}
	}
}
