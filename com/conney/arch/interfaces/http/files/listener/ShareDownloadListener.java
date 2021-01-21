package com.conney.arch.interfaces.http.files.listener;

public interface ShareDownloadListener
{
    void onDownloadBefore(ShareDownloadEvent event);

    void onDownload(ShareDownloadEvent event);

    void onShareDownload(ShareDownloadEvent event);

    void onLocalDownload(ShareDownloadEvent event);

    void onDownloadError(ShareDownloadEvent event);

    void onShareDownloadError(ShareDownloadEvent event);

    void onLocalDownloadError(ShareDownloadEvent event);
}
