package com.conney.arch.interfaces.http.files;

import java.nio.file.Path;

public interface FileMetadataOperator
{
    Path createMetaPath();

    FileMetadata newFileMetadata(Path path);

    FileMetadata getFileMetadata();
}
