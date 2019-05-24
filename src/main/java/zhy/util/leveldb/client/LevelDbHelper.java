package zhy.util.leveldb.client;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
import zhy.util.leveldb.query.Condition;
import zhy.util.leveldb.query.ConditionFilter;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;


/**
 * Leveldb operation,which is not about business.
 */
public class LevelDbHelper {
    private static final LevelDbHelper INDEX_DB = new LevelDbHelper("id_index");
    private static final String CHARSET = "UTF-8";
    private static String FILE_ROOT = System.getProperty("user.home") + "/.leveldb";

    /**
     * The name of entity to save is similar with the table name in SQL database.
     */
    private String entityName;
    /**
     *
     */
    private ConditionFilter filter = new ConditionFilter();

    public LevelDbHelper(String entityName) {
        this.entityName = entityName;
    }

    /**
     * Get value according to key
     *
     * @param key Key
     * @return Value
     */
    public String get(Object key) {
        DB db = getDb();
        byte[] value;
        String result = null;
        try {
            value = Objects.requireNonNull(db).get(key.toString().getBytes(CHARSET));
            result = value == null ? null : new String(value, CHARSET);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        autoClose(db);
        return result;
    }

    /**
     * Find key-value according to conditions.
     *
     * @param conditions Conditions
     * @param startRow   Start row number,including
     * @param endRow     End row number,excluding
     * @return Result map between key and value
     */
    public Map<String, String> find(List<Condition> conditions, int startRow, int endRow) {
        DB db = getDb();
        Map<String, String> result = new HashMap<>();

        //Read from a snapshot,and changed contents while reading cant be find.
        Snapshot snapshot = Objects.requireNonNull(db).getSnapshot();
        ReadOptions readOptions = new ReadOptions();
        //遍历中swap出来的数据，不应该保存在memtable中。
        readOptions.fillCache(false);
        readOptions.snapshot(snapshot);
        //默认snapshot为当前。
        DBIterator iterator = db.iterator(readOptions);
        int rowNo = 0;
        Map.Entry<byte[], byte[]> item;
        for (; iterator.hasNext(); ) {
            if ((endRow > 0 && rowNo > endRow)) break;
            item = iterator.next();
            String key = null;
            try {
                key = new String(item.getKey(), CHARSET);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            String value = null;
            try {
                value = new String(item.getValue(), CHARSET);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            if (filter.satisfies(value, conditions)) {
                if (rowNo >= startRow) result.put(key, value);
                rowNo++;
            }
        }
        autoClose(iterator, db);
        return result;
    }

    /**
     * Update if key exists,or insert,synchronously.
     *
     * @param toInsert Entities to put
     * @return The size to put
     */
    public synchronized int putOrAdd(Map<Object, String> toInsert) {
        DB db = getDb();
        //write后立即进行磁盘同步写
        WriteOptions writeOptions = new WriteOptions().sync(true);
        WriteBatch writeBatch = Objects.requireNonNull(db).createWriteBatch();
        toInsert.forEach((key, value) -> {
            try {
                if (value == null || value.trim().isEmpty()) {
                    throw new IllegalArgumentException("Empty value cant be stored by LevelDB");
                }
                writeBatch.put(key.toString().getBytes(CHARSET), value.getBytes(CHARSET));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });
        db.write(writeBatch, writeOptions);
        autoClose(writeBatch, db);
        return toInsert.size();
    }

    /**
     * Update if key exists,or insert,synchronously.
     *
     * @param key   Key to insert
     * @param toPut Entity to put
     */
    public synchronized void putOrAdd(Object key, String toPut) {
        DB db = getDb();
        WriteOptions writeOptions = new WriteOptions().sync(true);
        try {
            Objects.requireNonNull(db).put(key.toString().getBytes(CHARSET), toPut.getBytes(CHARSET), writeOptions);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        autoClose(db);
    }

    /**
     * Insert a new record.
     *
     * @param toInsert new record
     * @return the primary key
     */
    public int put(String toInsert) {
        int key = idNextAndIncrement();
        putOrAdd(key, toInsert);
        return key;
    }

    /**
     * Delete data according to key
     *
     * @param key Key to delete
     */
    public void delete(String key) {
        DB db = getDb();
        try {
            Objects.requireNonNull(db).delete(key.getBytes(CHARSET));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        autoClose(db);
    }

    /**
     * Delete data according to
     *
     * @param conditions Conditions
     * @return Number deleted
     */
    public int deleteBy(List<Condition> conditions) {
        Set<String> ids = find(conditions, -1, -1).keySet();
        ids.forEach(this::delete);
        return ids.size();
    }

    private DB getDb() {
        File file = new File(FILE_ROOT + '/' + entityName);
        Options options = new Options().createIfMissing(true);
        try {
            return Iq80DBFactory.factory.open(file, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void autoClose(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized int idNextAndIncrement() {
        String index = INDEX_DB.get(entityName);
        int nextId = index == null ? 1 : Integer.valueOf(index) + 1;
        INDEX_DB.putOrAdd(entityName, String.valueOf(nextId));
        return nextId;
    }

    public int deleteAndInit() {
        INDEX_DB.putOrAdd(entityName, "0");
        return deleteBy(null);
    }

    public int idNext() {
        String index = INDEX_DB.get(entityName);
        return index == null ? 1 : Integer.valueOf(index) + 1;
    }
}
