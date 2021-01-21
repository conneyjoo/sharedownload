package com.conney.arch.exceptions;

public class FileMD5NotMatchException extends DownloadException
{
    public FileMD5NotMatchException(String etag, String md5)
    {
        super(String.format("ETag[%s] and MD5[%s] not match", etag, md5), -1);
    }

    public FileMD5NotMatchException(String etag, String md5, int status)
    {
        super(String.format("ETag[%s] and MD5[%s] not match", etag, md5), status);
    }
}
