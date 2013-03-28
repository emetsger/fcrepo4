
package org.fcrepo.utils;

import static org.fcrepo.utils.FixityResult.FixityState.BAD_CHECKSUM;
import static org.fcrepo.utils.FixityResult.FixityState.BAD_SIZE;
import static org.fcrepo.utils.FixityResult.FixityState.SUCCESS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Properties;

import org.apache.poi.util.IOUtils;
import org.fcrepo.utils.infinispan.StoreChunkInputStream;
import org.fcrepo.utils.infinispan.StoreChunkOutputStream;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.binary.BinaryStore;
import org.modeshape.jcr.value.binary.BinaryStoreException;
import org.modeshape.jcr.value.binary.infinispan.InfinispanBinaryStore;
import org.slf4j.Logger;

public class LowLevelCacheEntry {

    private static final Logger logger = getLogger(LowLevelCacheEntry.class);

    private static final String DATA_SUFFIX = "-data";

    private final BinaryStore store;

    private final CacheStore low_level_store;

    private final BinaryKey key;

    public LowLevelCacheEntry(BinaryStore store, CacheStore low_level_store,
            BinaryKey key) {
        this.store = store;
        this.low_level_store = low_level_store;
        this.key = key;
    }

    public LowLevelCacheEntry(BinaryStore store, BinaryKey key) {
        this.store = store;
        this.low_level_store = null;
        this.key = key;
    }

    public boolean equals(final Object other) {
        if (other instanceof LowLevelCacheEntry) {
            final LowLevelCacheEntry that = (LowLevelCacheEntry) other;

            return this.key.equals(that.key) &&
                    this.store.equals(that.store) &&
                    ((this.low_level_store == null && that.low_level_store == null) || this.low_level_store
                            .equals(that.low_level_store));
        } else {
            return false;
        }
    }

    public InputStream getInputStream() throws BinaryStoreException {
        if (this.store instanceof InfinispanBinaryStore) {
            return new StoreChunkInputStream(low_level_store, key.toString() +
                    DATA_SUFFIX);
        } else {
            return this.store.getInputStream(key);
        }
    }

    public void storeValue(InputStream stream) throws BinaryStoreException,
            IOException {
        if (this.store instanceof InfinispanBinaryStore) {
            OutputStream outputStream =
                    new StoreChunkOutputStream(low_level_store, key.toString() +
                            DATA_SUFFIX);
            IOUtils.copy(stream, outputStream);
            outputStream.close();
        } else {
            // the BinaryStore will calculate a new key for us.
            this.store.storeValue(stream);
        }
    }

    public String getExternalIdentifier() {

        if (this.store instanceof InfinispanBinaryStore) {

            CacheStoreConfig config =
                    this.low_level_store.getCacheStoreConfig();

            String externalId = null;
            if (config instanceof AbstractCacheStoreConfig) {
                final Properties properties =
                        ((AbstractCacheStoreConfig) config).getProperties();
                if (properties.containsKey("id")) {
                    return properties.getProperty("id");
                }

            }

            if (externalId == null && config instanceof FileCacheStoreConfig) {
                externalId = ((FileCacheStoreConfig) config).getLocation();
            }

            if (externalId == null) {
                externalId = config.toString();
            }

            return this.store.getClass().getName() +
                    ":" +
                    this.low_level_store.getCacheStoreConfig()
                            .getCacheLoaderClassName() + ":" + externalId;

        } else {
            return this.store.toString();
        }
    }

    public FixityResult checkFixity(URI checksum, long size,
            MessageDigest digest) throws BinaryStoreException {
        FixityResult result = null;
        FixityInputStream ds = null;
        try {
            ds =
                    new FixityInputStream(getInputStream(),
                            (MessageDigest) digest.clone());

            result = new FixityResult(this);

            while (ds.read() != -1);

            result.computedChecksum =
                    ContentDigest
                            .asURI(digest.getAlgorithm(), ds.getMessageDigest().digest());
            result.computedSize = ds.getByteCount();
            result.dsChecksum = checksum;
            result.dsSize = size;
            if (!result.computedChecksum.equals(result.dsChecksum))
                result.status.add(BAD_CHECKSUM);
            if (result.dsSize != result.computedSize)
                result.status.add(BAD_SIZE);
            if (result.status.isEmpty()) result.status.add(SUCCESS);
            logger.debug("Got " + result.toString());
            ds.close();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return result;
    }
}