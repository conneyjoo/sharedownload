package com.conney.arch.exceptions;

import java.io.IOException;

public class DownloadException extends IOException
{
    private int status = 0;

    public DownloadException(String message, int status)
    {
        super(message.length() > 2047 ? message.substring(0, 2047) : message);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
