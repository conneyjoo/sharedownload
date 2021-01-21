package com.conney.arch.interfaces.http.files;

import java.io.IOException;

public class FileMetadataNotExistsException extends IOException
{
    public FileMetadataNotExistsException(Throwable cause)
    {
        super(cause);
    }
}
