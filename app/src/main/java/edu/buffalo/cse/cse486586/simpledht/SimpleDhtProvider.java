package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import java.io.Serializable;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

class Data implements Serializable {
    TreeMap<String,String> vals;
    String sdnrPort;
    String rcvrPort;
    DataTask task;
    String key;
    String val;
}

enum DataTask {Insert, Query, Delete, JoinRequest, Neighbors, Result, QueryGlobal, DeleteGlobal}

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = "SimpleDht";
    final int SERVER_PORT = 10000;
    static String LEADER = "5554";
    static String predPort;
    static String port;
    static String succPort;
    static final Uri CONTENT_URI = Uri.parse("edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider");
    TreeMap<String, String> nodeList = null;
    private DhtOpenHandler dhtStore;


    @Override
    public boolean onCreate() {
        nodeList = new TreeMap<String, String>();
        dhtStore = new DhtOpenHandler(getContext());
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        port = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "error creating socket: ", e);
        }
        nodeList.put(genHash(port), port);

        int positionOfNewNode = 0;
        positionOfNewNode = calculatePosition(genHash(port));
        if(positionOfNewNode-1<0)
            predPort = nodeList.get(nodeList.keySet().toArray()[nodeList.size()-1]);
        else
            predPort = nodeList.get(nodeList.keySet().toArray()[positionOfNewNode-1]);

        if(positionOfNewNode+1>nodeList.size()-1)
            succPort = nodeList.get(nodeList.keySet().toArray()[0]);
        else
            succPort = nodeList.get(nodeList.keySet().toArray()[positionOfNewNode+1]);

        if(!port.equals(LEADER))
            clientJoins();
        return true;
    }

    private int calculatePosition(String hash){
        int i = 0;
        for(String k : nodeList.keySet()){
            if(k.equals(hash))
                return i;
            i++;
        }
        return 0;
    }

    private void clientJoins() {
        try {
            Data joinMessage = prepareJoinMsg();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinMessage);
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
    }

    private Data prepareJoinMsg() {
        Data joinMessage = new Data();
        try{
            int positionOfNewNode = 0;
            positionOfNewNode = calculatePosition(genHash(port));
            if(positionOfNewNode-1<0)
                predPort = nodeList.get(nodeList.keySet().toArray()[nodeList.size()-1]);
            else
                predPort = nodeList.get(nodeList.keySet().toArray()[positionOfNewNode-1]);

            if(positionOfNewNode+1>nodeList.size()-1)
                succPort = nodeList.get(nodeList.keySet().toArray()[0]);
            else
                succPort = nodeList.get(nodeList.keySet().toArray()[positionOfNewNode+1]);
            joinMessage.task = DataTask.JoinRequest;
            joinMessage.vals = new TreeMap<String, String>();
            joinMessage.key = succPort;
            joinMessage.val = predPort;
            joinMessage.vals.put(genHash(port), port);
            joinMessage.sdnrPort = port;
            joinMessage.rcvrPort = LEADER;
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return joinMessage;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int result = 0;
        try{
            Data deleteMsg = prepareDeleteMsg(selection);
            if(selection.equals("*"))
                result = globalDelete(deleteMsg);
            else if(selection.equals("@"))
                result = localDelete();
            else
                result = normalDelete(selection, deleteMsg);
        }catch (Exception e){
            Log.e(TAG,"Error while deleting: ",e);
        }
        return result;
    }

    private Data prepareDeleteMsg(String selection) {
        Data deleteMsg = new Data();
        try {
            deleteMsg.vals = new TreeMap<String, String>();
            deleteMsg.vals.put(DataTask.Query.name(), selection);
            deleteMsg.sdnrPort = port;
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return deleteMsg;
    }

    private int normalDelete(String selection, Data deleteMsg) throws InterruptedException, java.util.concurrent.ExecutionException {
        int retVal = 0;
        try {
            String compare = (genHash(selection).compareTo(nodeList.lastKey()) <= 0)?nodeList.ceilingKey(genHash(selection)):nodeList.firstKey();
            if (compare.equals(genHash(port)))
                retVal = DeleteKeyMine(selection);
            else
                retVal = DeleteKeyNotMine(selection, deleteMsg);
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return retVal;
    }

    private int DeleteKeyNotMine(String selection, Data deleteMsg) throws InterruptedException, java.util.concurrent.ExecutionException {
        int retVal =0;
        try {
            String compare = (genHash(selection).compareTo(nodeList.lastKey()) <= 0)?nodeList.ceilingKey(genHash(selection)):nodeList.firstKey();
            deleteMsg.rcvrPort = nodeList.get(compare);
            deleteMsg.task = DataTask.Query;
            retVal = (Integer) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMsg).get();
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return retVal;
    }

    private int DeleteKeyMine(String selection) {
        int retVal = 0;
        try {
            retVal = dhtStore.deleteMessage(selection);
        } catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return retVal;
    }

    private int globalDelete(Data deleteMsg) throws InterruptedException, java.util.concurrent.ExecutionException {
        int retVal = 0;
        try {
            deleteMsg.task = DataTask.DeleteGlobal;
            retVal = (Integer) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMsg).get();
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return retVal;
    }

    public int localDelete(){
        int result = 0;
        try {
            result = dhtStore.deleteAll();
        } catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return result;
    }
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Uri retVal;
        long row = -1;
        try{
            retVal = getRowUri(values, row);
        } catch (Exception e){
            Log.e(TAG,"Insert Exception: ",e);
            retVal = null;
        }
        return retVal;
    }

    private Uri getRowUri(ContentValues values, long row) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        Uri retVal;
        try {
            String compare = (genHash(values.getAsString("key")).compareTo(nodeList.lastKey()) <= 0)?nodeList.ceilingKey(genHash(values.getAsString("key"))):nodeList.firstKey();
            if (compare.equals(genHash(port)))
                row = insertKeyMine(values);
            else
                row = insertKeyNotMine(values, row);
            return ContentUris.withAppendedId(CONTENT_URI, row);
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
            retVal = null;
        }
        return retVal;
    }

    private long insertKeyNotMine(ContentValues values, long row) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        try {
            Data insertMessage = prepareInsertMsg(values);
            Object result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMessage).get(1000, TimeUnit.MILLISECONDS);
            if (result != null) row = (Long) result;
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return row;
    }

    private Data prepareInsertMsg(ContentValues values) {
        Data insertMessage = new Data();
        try {
            insertMessage.sdnrPort = port;
            insertMessage.task = DataTask.Insert;
            insertMessage.vals = new TreeMap<String, String>();
            insertMessage.vals.put(values.getAsString("key"), values.getAsString("value"));
            String compare = (genHash(values.getAsString("key")).compareTo(nodeList.lastKey()) <= 0)?nodeList.ceilingKey(genHash(values.getAsString("key"))):nodeList.firstKey();
            insertMessage.rcvrPort = nodeList.get(compare);
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return insertMessage;
    }

    private long insertKeyMine(ContentValues values) {
        long row = 0;
        try {
            row = dhtStore.insertMsg(values.get("key").toString(), values.get("value").toString());
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return row;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Cursor retVal = null;
        try{
            Data queryMsg = prepareQueryMsg();
            if(selection.equals("*"))
                retVal = globalQuery(selection, queryMsg);
            else if(selection.equals("@"))
                retVal = localQuery();
            else
                retVal = normalQuery(selection, queryMsg);
        }catch (Exception e){
            Log.e(TAG,"Error while querying: ",e);
        }
        return retVal;
    }

    private Data prepareQueryMsg() {
        Data queryMsg = new Data();
        try {
            queryMsg.sdnrPort = port;
            queryMsg.vals = new TreeMap<String, String>();
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
        return queryMsg;
    }

    private Cursor globalQuery(String selection, Data queryMsg) throws InterruptedException, java.util.concurrent.ExecutionException {
        Cursor retVal = null;
        try {
            prepareGlobalQueryMsg(selection, queryMsg);
            retVal = (Cursor) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg).get();
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return  retVal;
    }

    private void prepareGlobalQueryMsg(String selection, Data queryMsg) {
        try {
            queryMsg.sdnrPort = port;
            queryMsg.task = DataTask.QueryGlobal;
            queryMsg.vals.put(DataTask.QueryGlobal.name(), selection);
        }catch (Exception e){
            Log.e(TAG,"Error: ",e);
        }
    }

    private Cursor normalQuery(String selection, Data queryMsg) throws InterruptedException, java.util.concurrent.ExecutionException {
        Cursor retVal = null;
        try {
            String compare = (genHash(selection).compareTo(nodeList.lastKey()) <= 0)?nodeList.ceilingKey(genHash(selection)):nodeList.firstKey();
            if (compare.equals(genHash(port)))
                retVal = queryKeyMine(selection);
            else
                retVal = queryKeyNotMine(selection, queryMsg);
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return retVal;
    }

    private Cursor queryKeyNotMine(String selection, Data queryMsg) throws InterruptedException, java.util.concurrent.ExecutionException {
        Cursor retVal = null;
        try {
            String compare = (genHash(selection).compareTo(nodeList.lastKey()) <= 0)?nodeList.ceilingKey(genHash(selection)):nodeList.firstKey();
            queryMsg.sdnrPort = port;
            queryMsg.vals.put(DataTask.Query.name(), selection);
            queryMsg.rcvrPort = nodeList.get(compare);
            queryMsg.task = DataTask.Query;
            retVal = (Cursor) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg).get();
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return retVal;
    }

    private Cursor queryKeyMine(String selection) {
        Cursor retVal = null;
        try {
            retVal = dhtStore.getMessageByKey(selection);
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return retVal;
    }

    private Cursor localQuery(){
        Cursor retVal = null;
        try {
            retVal = dhtStore.getAll();
        } catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return retVal;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input){
        try{
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        }catch( NoSuchAlgorithmException e){
            Log.e(TAG,"Error Algo: ", e);
        }catch (Exception e){
            Log.e(TAG, "Error: "+ e);
        }
        return "";
    }


    private class ClientTask extends AsyncTask<Data, Void, Object> {
        @Override
        protected Object doInBackground(Data... msgs) {
            Data msg = msgs[0];
            Object retVal = null;
            try {
                retVal = executeClient(msg, retVal);
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
            return retVal;
        }

        private Object executeClient(Data msg, Object retVal) {
            try {
                if (msg.task == DataTask.DeleteGlobal)
                    retVal = sendDeleteAllMsg(msg);
                else if (msg.task == DataTask.QueryGlobal)
                    retVal = sendQueryAllMsg(msg);
                else if (msg.task == DataTask.Neighbors)
                    sendNeighUpdateMsg(msg);
                else if (msg.task == DataTask.Query)
                    retVal = sendQueryMsg(msg);
                else
                    retVal = sendNormalMsg(msg);
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
            return retVal;
        }

        private Object sendQueryMsg(Data msg) {
            Data msgRec = (Data) sendNormalMsg(msg);
            MatrixCursor result = new MatrixCursor(new String[]{"key","value"});
            try{
                for(Map.Entry<String, String> entry : msgRec.vals.entrySet())
                    result.addRow(new Object[]{entry.getKey(),entry.getValue()});
                return result;
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
            return  null;
        }

        private void sendNeighUpdateMsg(Data msg) {
            try{
                for(String remotePort: nodeList.values()){
                    msg.rcvrPort = remotePort;
                    sendNormalMsg(msg);
                }
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private Object sendQueryAllMsg(Data msg) {
            try {
                TreeMap<String, String> values = gatherQueryAllResult(msg);
                MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});
                for (Map.Entry<String, String> entry : values.entrySet())
                    result.addRow(new Object[]{entry.getKey(), entry.getValue()});
                return result;
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
            return null;
        }

        private TreeMap<String, String> gatherQueryAllResult(Data msg) {
            TreeMap<String, String> values = new TreeMap<String, String>();
            try{
                for(String receiverPort: nodeList.values()){
                    msg.rcvrPort = receiverPort;
                    if(receiverPort.equals(port)){
                        Cursor cursor = localQuery();
                        while (cursor.moveToNext())
                            values.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                    }else{
                        Data msgRec1 = (Data) sendNormalMsg(msg);
                        values.putAll(msgRec1.vals);
                    }
                }
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
            return values;
        }

        private Object sendDeleteAllMsg(Data msg) {
            int i = 0;
            try {
                for (String remotePort : nodeList.values()) {
                    msg.rcvrPort = remotePort;
                    if (remotePort.equals(SimpleDhtProvider.port))
                        i += localDelete();
                    else
                        i += (Integer) sendNormalMsg(msg);
                }
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
            return i;
        }

        protected Object sendNormalMsg(Data msg){
            Object response = null;
            int remote = Integer.parseInt(msg.rcvrPort) *2;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remote);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                response = send(msg, response, socket, out, in);
            }
            catch (Exception e){
                Log.e(TAG,"Error creating socket: "+e);
            }
            return response;
        }

        private Object send(Data msg, Object response, Socket socket, ObjectOutputStream out, ObjectInputStream in) throws IOException {
            try {
                out.writeObject(msg);
                out.flush();
                socket.getReceiveBufferSize();
                response = in.readObject();
            } catch (Exception e) {
                Log.e(TAG, "Error while writing: "+ e);
            } finally {
                in.close();
                out.close();
                socket.close();
            }
            return response;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, Data, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true){
                Socket socket =  null;
                ObjectOutputStream opObject = null;
                ObjectInputStream ipObject  = null;
                try {
                    socket =  serverSocket.accept();
                    opObject = new ObjectOutputStream(socket.getOutputStream());
                    ipObject  = new ObjectInputStream(socket.getInputStream());
                    Data rcvd = (Data) ipObject.readObject();
                    executeServer(opObject, rcvd);
                } catch (Exception e) {
                    Log.e(TAG, "Exception in ServerSocket: ", e);
                }
                finally {
                    try {
                        ipObject.close();
                        opObject.close();
                        socket.close();
                    }
                    catch(Exception e){
                        Log.e(TAG, "Exception while closing ServerSocket: ", e);
                    }
                }
            }
        }

        private void executeServer(ObjectOutputStream opObject, Data rcvd) throws IOException {
            try {
                switch (rcvd.task) {
                    case Insert:
                        receivedInsertMsg(opObject, rcvd);
                        break;
                    case Delete:
                        receivedNormalDeleteMsg(opObject, rcvd);
                        break;
                    case JoinRequest:
                        receivedJoinMsg(opObject, rcvd);
                        break;
                    case Neighbors:
                        receivedNeighUpdateMsg(opObject, rcvd);
                        break;
                    case Query:
                        receivedNormalQueryMsg(opObject, rcvd);
                        break;
                    case QueryGlobal:
                        receivedGlobalQueryMsg(opObject, rcvd);
                        break;
                    case DeleteGlobal:
                        receivedGlobalDeleteMsg(opObject);
                        break;
                }
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private void receivedJoinMsg(ObjectOutputStream opObject, Data rcvd) throws IOException {
            nodeList.put(rcvd.vals.firstKey(), rcvd.vals.firstEntry().getValue());
            try {
                Data msg = prepareNeighUpdate();
                opObject.writeObject(null);
                opObject.flush();
                publishProgress(msg);
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private Data prepareNeighUpdate() {
            Data msg = new Data();
            try {
                msg.task = DataTask.Neighbors;
                msg.sdnrPort = port;
                msg.vals = nodeList;
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
            return msg;
        }

        private void receivedNeighUpdateMsg(ObjectOutputStream opObject, Data rcvd) throws IOException {
            nodeList = rcvd.vals;
            try {
                opObject.writeObject(null);
                opObject.flush();
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private void receivedInsertMsg(ObjectOutputStream opObject, Data rcvd) throws IOException {
            try{
                ContentValues values = new ContentValues();
                values.put("key",rcvd.vals.firstEntry().getKey());
                values.put("value", rcvd.vals.firstEntry().getValue());
                Long rowID = dhtStore.insertMsg(values.get("key").toString(), values.get("value").toString());
                opObject.writeObject(rowID);
                opObject.flush();
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private void receivedNormalDeleteMsg(ObjectOutputStream opObject, Data rcvd) throws IOException {
            try {
                Integer count = dhtStore.deleteMessage(rcvd.vals.firstEntry().getValue());
                opObject.writeObject(count);
                opObject.flush();
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private void receivedNormalQueryMsg(ObjectOutputStream opObject, Data rcvd) throws IOException {
            String searchValue = rcvd.vals.firstEntry().getValue();
            Cursor cursor = dhtStore.getMessageByKey(searchValue);
            rcvd.task = DataTask.Result;
            rcvd.vals = new TreeMap<String, String>();
            try{
                while (cursor.moveToNext())
                    rcvd.vals.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                opObject.writeObject(rcvd);
                opObject.flush();
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private void receivedGlobalQueryMsg(ObjectOutputStream opObject, Data rcvd) throws IOException {
            Cursor cursor = dhtStore.getAll();
            rcvd.task = DataTask.Result;
            rcvd.vals = new TreeMap<String, String>();
            try{
                while (cursor.moveToNext())
                    rcvd.vals.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                opObject.writeObject(rcvd);
                opObject.flush();
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        private void receivedGlobalDeleteMsg(ObjectOutputStream opObject) throws IOException {
            try{
                Integer count = dhtStore.deleteAll();
                opObject.writeObject(count);
                opObject.flush();
            }catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }

        protected void onProgressUpdate(Data...msgs) {
            try {
                Data msg = msgs[0];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            } catch (Exception e){
                Log.e(TAG, "Error: "+ e);
            }
        }
    }

}