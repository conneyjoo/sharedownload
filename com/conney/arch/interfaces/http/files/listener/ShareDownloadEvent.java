package com.conney.arch.interfaces.http.files.listener;

import com.conney.arch.interfaces.http.files.FileMetadata;

import java.io.File;
import java.net.URI;

public class ShareDownloadEvent
{
    public ShareDownloadEventType type;

    public URI uri;

    public File file;

    public String relativePath;

    public FileMetadata fileMetadata;

    public String message;

    public String exceptionName;

    public long startTime;

    public long duration;

    public ShareDownloadEvent(ShareDownloadEventType type)
    {
        this.type = type;
    }

    public static ShareDownloadEvent create(ShareDownloadEventType type)
    {
        return new ShareDownloadEvent(type);
    }

    public ShareDownloadEvent(FileMetadata fileMetadata)
    {
        this.fileMetadata = fileMetadata;
    }

    public void destroy()
    {
        type = null;
        uri = null;
        fileMetadata = null;
        message = null;
    }

    @Override
    public String toString()
    {
        return "ShareDownloadEvent{" + "uri=" + uri + '}';
    }
}
