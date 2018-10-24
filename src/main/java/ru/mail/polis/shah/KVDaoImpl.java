package ru.mail.polis.shah;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class KVDaoImpl implements KVDao {
    private final File dir;
    private final DB db;
    private final HTreeMap<byte[], byte[]> map;

    public KVDaoImpl(File dir) {
        this.dir = dir;
        this.db = DBMaker
                .fileDB(new File(dir, "mapdb.db"))
                .fileChannelEnable()
                .make();
        this.map = db
                .hashMap("storage")
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] res = map.get((key));
        if (res == null) {
            throw new NoSuchElementException(new String(key, StandardCharsets.UTF_8));
        }
        return res;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put((key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        map.remove((key));
    }

    @Override
    public void close() throws IOException {
        map.close();
        db.close();
    }
}
