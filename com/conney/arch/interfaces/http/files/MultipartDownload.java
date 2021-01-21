package com.conney.arch.interfaces.http.files;

import java.net.URI;

/**
 * 分片下载,加快外网文件下载速度.
 * 当有下载请求加入时进行分裂
 */
public class MultipartDownload extends ShareDownload
{
    public MultipartDownload(String url, String storePath)
    {
        super(url, storePath);
    }

    public MultipartDownload(URI uri, String storePath)
    {
        super(uri, storePath);
    }

    public void split()
    {
    }

    class FlowPart
    {

    }
}
