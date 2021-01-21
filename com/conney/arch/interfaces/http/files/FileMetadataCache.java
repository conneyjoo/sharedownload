package com.conney.arch.interfaces.http.files;

import com.conney.arch.utils.LRULinkedHashMap;

import java.nio.file.Path;

public class FileMetadataCache
{
    private static final int DEFAULT_MAX_CACHE_CAPACITY = 2 << 16;

    private LRULinkedHashMap<Path, FileMetadata> caches = new LRULinkedHashMap<>(DEFAULT_MAX_CACHE_CAPACITY);

    public synchronized FileMetadata get(FileMetadataOperator operator)
    {
        final Path path = operator.createMetaPath();

        return caches.computeIfAbsent(path, k ->
        {
            try
            {
                FileMetadata fileMetadata = operator.newFileMetadata(path);
                fileMetadata.read();
                return fileMetadata;
            }
            catch (FileMetadataNotExistsException e)
            {
                return null;
            }
        });
    }

    public void add(FileMetadataOperator operator)
    {
        FileMetadata fileMetadata = operator.getFileMetadata();

        if (fileMetadata != null)
        {
            caches.put(fileMetadata.getPath(), fileMetadata);
        }
    }
}
