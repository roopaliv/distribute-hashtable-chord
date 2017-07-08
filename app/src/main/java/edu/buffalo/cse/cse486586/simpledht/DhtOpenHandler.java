package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by paali on 4/4/17.
 */
import android.database.sqlite.*;
import android.content.*;
import android.database.Cursor;
import android.util.Log;
public class DhtOpenHandler extends SQLiteOpenHelper{
    private static final int version = 1;
    private static final String tableName = "DhtTable";
    private static final String dbName = "DhtDB";
    private static final String keyColumn = "key";
    private static final String valueColumn = "value";
    private static final String messageCreateStatement = "CREATE TABLE " + tableName + " (" + keyColumn + " TEXT PRIMARY KEY, " + valueColumn + " TEXT);";

    DhtOpenHandler(Context context) {
        super(context, dbName, null, version);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(messageCreateStatement);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + tableName);
        onCreate(db);
    }

    public Cursor getMessageByKey(String key) {
        try {
            SQLiteDatabase db = this.getReadableDatabase();
            Cursor returnedRows =  db.rawQuery( "select * from "+tableName+" where "+keyColumn+"='"+key+"'", null );
            return returnedRows;
        }
        catch (Exception e){
            Log.e("error getMessageByKey: ", e.getMessage());
            return  null;
        }
    }
    public Cursor getAll() {
        try {
            SQLiteDatabase db = this.getReadableDatabase();
            Cursor returnedRows =  db.rawQuery( "select * from "+tableName, null );
            return returnedRows;
        }
        catch (Exception e){
            Log.e("error getAll: ", e.getMessage());
            return  null;
        }
    }
    public boolean updateMessage (String key, String value) {
        try {
            SQLiteDatabase db = this.getWritableDatabase();
            ContentValues contentValues = new ContentValues();
            contentValues.put("value", value);
            db.update(tableName, contentValues, keyColumn + " = ? ", new String[]{key});
            return true;
        }
        catch (Exception e){
            Log.e("error updateMessage: ", e.getMessage());
            return false;
        }
    }

    public long insertMsg (String key, String value) {
        try {
            SQLiteDatabase db = this.getWritableDatabase();
            ContentValues contentValues = new ContentValues();
            contentValues.put("key", key);
            contentValues.put("value", value);
            long rowID = db.insertWithOnConflict(tableName, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
            return rowID;
        }
        catch(Exception e) {
            Log.e("error insertMessage: ", e.getMessage());
            return 0;
        }
    }

    public int deleteMessage(String key){
        try {
            SQLiteDatabase db = this.getWritableDatabase();
            int i = db.delete(tableName, keyColumn + "=?", new String[]{key});
            return i;
        }
        catch (Exception e){
            Log.e("error deleteMessage: ", e.getMessage());
            return 0;
        }
    }

    public int deleteAll(){
        try {
            SQLiteDatabase db = this.getWritableDatabase();
            int no = db.delete(tableName, "1", null);
            //int no = db.delete(tableName, null, null);
            return no;
        }
        catch (Exception e){
            Log.e("error deleteAll: ", e.getMessage());
            return 0;
        }
    }
}
