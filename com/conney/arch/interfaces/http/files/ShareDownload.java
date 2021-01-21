package com.conney.arch.interfaces.http.files;

import com.aliyun.oss.common.utils.DateUtil;
import com.aliyun.oss.internal.OSSHeaders;
import com.conney.arch.exceptions.DownloadException;
import com.conney.arch.exceptions.FileMD5NotMatchException;
import com.conney.arch.interfaces.http.files.listener.ShareDownloadEvent;
import com.conney.arch.interfaces.http.files.listener.ShareDownloadEventType;
import com.conney.arch.interfaces.http.files.listener.ShareDownloadListener;
import com.conney.arch.utils.UUIDTool;
import com.google.common.collect.Maps;
import io.netty.handler.codec.http.HttpScheme;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.coyote.http2.StreamException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.tomcat.util.http.fileupload.util.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.conney.arch.interfaces.http.files.FileAssist.*;
import static org.apache.http.HttpStatus.*;
import static org.apache.tomcat.util.http.fileupload.IOUtils.closeQuietly;

/**
 * http文件共享下载
 * 把多个相同的url合并成一个外网下载,最先的请求的走外网下载,后面的请求从第一个外网下载中获取数据
 * 为了避免实际从外网下载的请求(也就是第一个请求)中断导致后面的请求没有数据来源,ShareDownload采用从先到后的顶替顺序,由下一个请求延续外网下载提供后面请求的数据来源
 * 如:客户端下载的请求顺序为[A,B,C]
 *  - A从外网下数据, B,C从A获取数据
 *  - A发生异常, B顶替A从外网下载数据, C从B中获取数据
 *  - B发生异常, C顶替B从外网下载数据
 *
 *  Range说明: 只有本地下载才支持Range,也就是盒子上有这个文件时
 */
public class ShareDownload implements FileMetadataOperator
{
    protected static final Logger logger = LoggerFactory.getLogger(ShareDownload.class);

    /**
     * 缓冲区大小
     */
    private static final int BUFFER_SIZE = 1024 * 4;

    /**
     * 元数据文件后缀名
     */
    private static final String META_SUFFIX = ".meta";

    /**
     * http client最大连接路由数
     */
    private static final int HTTP_CLIENT_CONN_PER_ROUTE = 512;

    /**
     * http client最大连接总数
     */
    private static final int HTTP_CLIENT_MAX_CONN_TOTAL = 512;

    /**
     * http client socket操作超时
     */
    private static final int HTTP_CLIENT_SOCKET_TIMEOUT = 60000;

    /**
     * http client连接超时
     */
    private static final int HTTP_CLIENT_CONNECT_TIMEOUT = 15000;

    /**
     * transfer线程池核心线程数
     */
    private static final int TRANSFER_POOL_CORE = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * transfer线程池最大线程数
     */
    private static final int TRANSFER_POOL_MAX = TRANSFER_POOL_CORE;

    /**
     * transfer任务队列大小
     */
    private static final int TRANSFER_POOL_TASK_SIZE = Integer.MAX_VALUE;

    /**
     * http commons下载组件
     */
    protected static CloseableHttpClient httpClient = HttpClients.custom().setMaxConnPerRoute(HTTP_CLIENT_CONN_PER_ROUTE).setMaxConnTotal(HTTP_CLIENT_MAX_CONN_TOTAL).disableContentCompression().disableAutomaticRetries().build();

    /**
     * http client请求配置
     */
    protected static RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(HTTP_CLIENT_SOCKET_TIMEOUT).setConnectTimeout(HTTP_CLIENT_CONNECT_TIMEOUT).build();

    /**
     * ShareDownload池(存放下载中的ShareDownload)
     */
    private static Map<URI, ShareDownload> pool = Maps.newConcurrentMap();

    /**
     * 事件任务调度器
     */
    private static ExecutorService executor = Executors.newFixedThreadPool(8);

    /**
     * 处理加入共享下载的请求的任务队列
     */
    private static LinkedBlockingQueue transferQueue = new LinkedBlockingQueue(TRANSFER_POOL_TASK_SIZE);

    /**
     * 处理加入共享下载的请求的线程池
     */
    private static ExecutorService transferExecutor = Executors.newFixedThreadPool(128);

    /**
     * 异步下载执行器
     */
    private static ExecutorService downloadExecutor = Executors.newCachedThreadPool();

    /**
     * 下载的uri
     */
    protected URI uri;

    /**
     * 文件(以url域名后面的内容为路径)
     */
    protected File file;

    /**
     * 请求路径
     */
    protected String relativePath;

    /**
     * 非原文件,不需要校验MD5
     */
    protected boolean nonRaw = false;

    /**
     * 当前url有多少个请求下载
     */
    protected AtomicInteger count = new AtomicInteger(0);

    /**
     * 当前下载文件传输位置
     */
    protected volatile long position = 0;

    /**
     * 文件的元数据
     */
    protected volatile FileMetadata fileMetadata;

    /**
     * 非首个下载的请求对象集合
     */
    protected Queue<Transfer> transfers = new ConcurrentLinkedQueue<>();

    /**
     * 是否下载完成
     */
    protected boolean success = false;

    /**
     * start下载完成标记
     */
    protected AtomicBoolean completed = new AtomicBoolean(false);

    /**
     * 下载事件监听
     */
    protected ShareDownloadListener listener;

    public ShareDownload(String url, String storeHome)
    {
        if (StringUtils.isEmpty(url))
        {
            throw new IllegalArgumentException("url not be null");
        }

        if (StringUtils.isEmpty(storeHome))
        {
            throw new IllegalArgumentException("storeHome not be null");
        }

        this.uri = URI.create(url);
        this.relativePath = extractFilename();
        this.file = new File(storeHome + relativePath);
    }

    public ShareDownload(URI uri, String storePath)
    {
        if (uri == null)
        {
            throw new IllegalArgumentException("url not be null");
        }

        if (StringUtils.isEmpty(storePath))
        {
            throw new IllegalArgumentException("storePath not be null");
        }

        this.uri = uri;
        this.relativePath = extractFilename();
        this.file = new File(storePath + relativePath);
    }

    public String extractFilename()
    {
        return extractFilename(uri);
    }

    public String extractFilename(URI uri)
    {
        String path = uri.getPath();
        String query = uri.getQuery();
        String filename;

        if (StringUtils.isNotEmpty(query))
        {
            String params = DigestUtils.md5Hex(query.getBytes());
            int index = path.lastIndexOf(".");
            filename = index != -1 ? path.substring(0, index) + "_" + params + path.substring(index) : path + "_" + params;
            nonRaw = true;
        }
        else
        {
            filename = path;
        }

        return filename;
    }

    /**
     * 下载文件方法
     * 如果本地存在则从本地获取数据返回到客户端,否则进入外网下载流程
     * 外网下载流程: 当第一次请求则开始外网下载,第二次以上请求则加入到共享下载
     *
     * @param request  请求下载对象
     * @param response 返回数据对象
     */
    public void download(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        if (exists())
        {
            local(request, response);
            return;
        }

        if (isMediaFile())
        {
            redirect(response).start(true);
            return;
        }

        ShareDownload shareDownload;
        if ((shareDownload = downloading()) == null)
        {
            suspend(request, response).start();
        }
        else
        {
            shareDownload.suspend(request, response);
        }
    }

    /**
     * 下载中
     * 加入ShareDownload池
     *
     * @return ShareDownload
     */
    protected ShareDownload downloading() { return pool.putIfAbsent(uri, this); }

    /**
     * 下载完成后
     * 从ShareDownload池中移除
     * 当前下载记录数减一
     */
    protected void downloaded()
    {
        pool.remove(uri);
    }

    /**
     * 判断本地是否存在该文件
     *
     * @return 是否存在
     */
    protected boolean exists()
    {
        return file.exists();
    }

    /**
     * 判断本地是否媒体文件
     *
     * @return 是否存在
     */
    protected boolean isMediaFile()
    {
        int index = relativePath.indexOf(".");

        if (index != -1)
        {
            String suffix = relativePath.substring(relativePath.indexOf("."));
            return suffix.equalsIgnoreCase(".mp4") || suffix.equalsIgnoreCase(".cwp");
        }
        else
        {
            return false;
        }
    }

    /**
     * 从本地获取文件下载
     *
     * @param request 请求数据对象
     * @param response 返回数据对象
     */
    protected void local(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        if (logger.isInfoEnabled())
        {
            logger.info("local downloading[{}]", uri);
        }

        sendRedirect(response, String.format("%s://%s%s", HttpScheme.HTTP.name(), request.getServerName(), relativePath));
    }

    /**
     * 开始下载(非后台下载)
     */
    public void start()
    {
        start(false);
    }

    /**
     * 开始下载
     * @param background 是否后台下载
     */
    public void start(boolean background)
    {
        if (background)
        {
            downloadExecutor.execute(() -> start(null, 0));
        }
        else
        {
            start(null, 0);
        }
    }

    /**
     * 开始下载
     * 下载流程: 读取url中数据 -> 写入本地临时文件 & response写入到metadata -> 文件是否完整(如etag为md5) -> 临时文件名改成url文件名
     * 如url请求的http header中带有etag为md5时进行md5校验,没有则不校验
     * 最后重新检测一遍transfers是否全部下载完成,如:在外网下载完之后加入的下载的请求情况
     *
     * @param resumeFile 断点续传文件
     * @param pos   断点续传文件位置
     */
    public void start(Path resumeFile, long pos)
    {
        if (logger.isInfoEnabled())
        {
            logger.info("start downloading [uri={}\tresumeFile={}\tpos={}]", uri, resumeFile, pos);
        }

        CloseableHttpResponse chp = null;
        InputStream in = null;
        RandomAccessFile fout = null;
        Path temp = resumeFile;
        Signal signal = new Signal(temp, pos);
        long startTime = now();
        long fetchNow = startTime;
        DownloadException de = null;

        completed.set(false);

        try
        {
            if (logger.isInfoEnabled())
            {
                logger.info("fetch source [uri={}]", uri);
            }

            chp = source(pos);

            if (chp == null)
            {
                return;
            }

            if (fileMetadata == null)
            {
                fileMetadata = newFileMetadata();
                fileMetadata.setCreateTime(startTime);
                fileMetadata.readResponse(chp, relativePath);
            }

            fireEvent(ShareDownloadEventType.DOWNLOAD_BEFORE, fileMetadata);

            if (temp == null)
            {
                temp = createTempFile(file.toPath());
                signal.path = temp;
            }

            in = chp.getEntity().getContent();
            fout = new RandomAccessFile(temp.toFile(), "rw");

            FileMetadata.Range range = pos > 0 ? fileMetadata.getRange(chp) : null;
            String etag = nonRaw ? null : fileMetadata.getMd5();
            boolean hasETag = StringUtils.isNotEmpty(etag);

            if (range != null && range.start > -1)
            {
                position = range.start;
                fout.seek(position);
            }

            writeStream(in, fout, signal);
            String md5 = hasETag ? md5Sum(fout) : null;

            if (logger.isInfoEnabled())
            {
                logger.info("fetch source completed [uri={}\tduration={}\tetag={}\tmd5={}]", uri, now() - fetchNow, etag, md5);
            }

            if (hasETag && !etag.equalsIgnoreCase(md5))
            {
                throw new FileMD5NotMatchException(etag, md5, SC_CONFLICT);
            }

            fileMetadata.write();
            copy(temp.toFile(), file);
            success = true;
            long duration = now() - startTime;
            fileMetadata.incrDownloadTime(duration - fileMetadata.getWriteLocalTime());
            fireEvent(ShareDownloadEventType.DOWNLOAD, fileMetadata, fileMetadata.getCreateTime(), duration);
        }
        catch (DownloadException e)
        {
            logger.warn("download error({}): {} [uri={}]", e.getStatus(), e.getMessage(), uri);
            fireErrorEvent((de = e).getMessage(), de.getClass().getName(), startTime);
        }
        catch (Throwable e)
        {
            logger.error("IO error: {} [uri={}]", e.getMessage(), uri, e);

            if (position > pos)
            {
                start(temp, position);
            }
            else
            {
                fireErrorEvent(e.getMessage(), e.getClass().getName(), startTime);
            }
        }
        finally
        {
            if (!completed.get())
            {
                closeQuietly(chp);
                closeQuietly(in);
                closeQuietly(fout);

                if (logger.isInfoEnabled())
                {
                    logger.info("release [success={}\turi={}]", success, uri);
                }

                synchronized (this)
                {
                    if (de != null)
                    {
                        sendErrorMessages(de.getStatus(), de.getMessage());
                    }

                    downloaded();
                    windUp(signal);
                    completed.set(true);

                    if (logger.isInfoEnabled())
                    {
                        logger.info("completed [uri={}\tcount={}\ttransfer size={}]", uri, getCount(), transfers.size());
                    }
                }
            }
        }
    }

    private void windUp(Signal signal)
    {
        Transfer transfer;
        while ((transfer = transfers.poll()) != null)
        {
            if (success) transfer.end(signal);
            else transfer.terminte();
        }
    }

    private void sendErrorMessages(int statusCode, String message)
    {
        for (Transfer transfer : transfers)
        {
            transfer.sendErrorMessage(statusCode, message);
        }
    }

    private void writeStream(InputStream in, RandomAccessFile fout, Signal signal) throws IOException
    {
        int len;
        byte[] buffer = new byte[BUFFER_SIZE];

        while ((len = in.read(buffer)) != -1)
        {
            writeStream(fout, signal, buffer, 0, len);
        }
    }

    private void writeStream(RandomAccessFile fout, Signal signal, byte[] buffer, int off, int len) throws IOException
    {
        long duration = now();
        fout.write(buffer, off, len);
        position += len;

        signal.position = position;
        transfers.stream().forEach((e) -> e.write(signal));

        fileMetadata.incrWriteLocalTime(now() - duration);
    }

    protected void sendRedirect(HttpServletResponse response, String url) throws IOException
    {
        if (response != null)
        {
            FileMetadata.setCors(response);
            response.sendRedirect(url);
        }
    }

    public void sendError(HttpServletResponse response, int sc, String msg)  throws IOException
    {
        if (response != null)
        {
            FileMetadata.setCors(response);
            response.sendError(sc, msg);
        }
    }

    protected ShareDownload redirect(HttpServletResponse response) throws IOException
    {
        return redirect(response, getURL());
    }

    protected ShareDownload redirect(HttpServletResponse response, String url) throws IOException
    {
        sendRedirect(response, url);
        return this;
    }

    /**
     * 获取元数据文件路径
     *
     * @return 文件路径
     */
    public Path createMetaPath()
    {
        return Paths.get(file.getAbsolutePath() + META_SUFFIX);
    }

    protected CloseableHttpResponse source() throws IOException
    {
        return source(0);
    }

    /**
     * http(get)请求下载文件
     *
     * @param position 断点续传位置
     * @return http报文
     */
    protected CloseableHttpResponse source(long position) throws IOException
    {
        HttpGet httpGet = new HttpGet(getURL());
        CloseableHttpResponse chp;
        httpGet.setConfig(requestConfig);
        httpGet.setHeader(OSSHeaders.DATE, DateUtil.formatRfc822Date(new Date()));
        if (position > 0) httpGet.setHeader(FileMetadata.RANGE_HEADER_NAME, "bytes=" + position + "-");

        try
        {
            if ((chp = httpClient.execute(httpGet)) != null)
            {
                int sc = chp.getStatusLine().getStatusCode();
                if (sc != SC_OK && sc != SC_PARTIAL_CONTENT)
                {
                    String error = Streams.asString(chp.getEntity().getContent());
                    httpGet.abort();
                    throw new DownloadException("http status code "+ sc + " error: " + error, sc);
                }
            }

            return chp;
        }
        catch (Throwable e)
        {
            httpGet.abort();
            throw e;
        }
    }

    private void addTransfer(HttpServletRequest request)
    {
        Transfer transfer = new Transfer(request);
        count.incrementAndGet();
        transfers.add(transfer);

        if (logger.isInfoEnabled())
        {
            logger.info("transfer({}) downloading({}) [success={}\turi={}]", transfer.identity, getCount(), success, uri);
        }
    }

    /**
     * 添加非首个下载的对象,当前下载请求加一
     *
     * 下载收尾工作完成但此时新加入进来的请求会卡在同步块中,completed.set(true)目的为了让这些请求重新回到download逻辑,否则在收尾工作完成后进入addTransfer的请求将永远不被处理
     *
     * @throws IOException
     */
    protected ShareDownload suspend(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        synchronized (this)
        {
            if (completed.compareAndSet(true, false))
            {
                download(request, response);
                completed.set(true);
            }
            else
            {
                addTransfer(request);
            }
        }

        return this;
    }

    private ShareDownloadEvent createEvent(ShareDownloadEventType type)
    {
        return ShareDownloadEvent.create(type);
    }

    public void setListener(ShareDownloadListener listener)
    {
        this.listener = listener;
    }

    public void fireErrorEvent(String message, String exceptionType, long startTime)
    {
        if (fileMetadata != null) fileMetadata.incrDownloadTime((now() - startTime - fileMetadata.getWriteLocalTime()));
        fireEvent(ShareDownloadEventType.DOWNLOAD_ERROR, fileMetadata, message, exceptionType);
    }

    public void fireEvent(ShareDownloadEvent event, boolean async)
    {
        if (async)
        {
            executor.submit(() -> fireEvent(event));
        }
        else
        {
            fireEvent(event);
        }
    }

    public void fireEvent(ShareDownloadEvent event)
    {
        if (listener != null)
        {
            if (event.type == ShareDownloadEventType.DOWNLOAD_BEFORE)
            {
                listener.onDownloadBefore(event);
            }
            else if (event.type == ShareDownloadEventType.DOWNLOAD)
            {
                listener.onDownload(event);
            }
            else if (event.type == ShareDownloadEventType.DOWNLOAD_ERROR)
            {
                listener.onDownloadError(event);
            }
            else if (event.type == ShareDownloadEventType.SHARED_DOWNLOAD)
            {
                listener.onShareDownload(event);
            }
            else if (event.type == ShareDownloadEventType.SHARED_DOWNLOAD_ERROR)
            {
                listener.onShareDownloadError(event);
            }
            else if (event.type == ShareDownloadEventType.LOCAL_DOWNLOAD)
            {
                listener.onLocalDownload(event);
            }
            else if (event.type == ShareDownloadEventType.LOCAL_DOWNLOAD_ERROR)
            {
                listener.onLocalDownloadError(event);
            }

            event.destroy();
        }
    }

    private void fireEvent(ShareDownloadEventType type, FileMetadata fileMetadata)
    {
        fireEvent(type, fileMetadata, null, -1, -1, true);
    }

    private void fireEvent(ShareDownloadEventType type, FileMetadata fileMetadata, long startTime, long duration)
    {
        fireEvent(type, fileMetadata, null, startTime, duration, true);
    }

    private void fireEvent(ShareDownloadEventType type, URI uri, FileMetadata fileMetadata, long startTime, long duration)
    {
        fireEvent(type, uri, fileMetadata, null, null, startTime, duration, true);
    }

    private void fireEvent(ShareDownloadEventType type, String message)
    {
        fireEvent(type, null, message, -1, -1, true);
    }

    private void fireEvent(ShareDownloadEventType type, String message, String exceptionName)
    {
        fireEvent(type, null, message, exceptionName, -1, -1, true);
    }

    private void fireEvent(ShareDownloadEventType type, FileMetadata fileMetadata, String message)
    {
        fireEvent(type, fileMetadata, message, -1, -1, true);
    }

    private void fireEvent(ShareDownloadEventType type, FileMetadata fileMetadata, String message, String exceptionName)
    {
        fireEvent(type, fileMetadata, message, exceptionName, -1, -1, true);
    }

    private void fireEvent(ShareDownloadEventType type, FileMetadata fileMetadata, String message, long startTime, long duration, boolean async)
    {
        fireEvent(type, fileMetadata, message, "", startTime, duration, async);
    }

    private void fireEvent(ShareDownloadEventType type, FileMetadata fileMetadata, String message, String exceptionName, long startTime, long duration, boolean async)
    {
        fireEvent(type, uri, fileMetadata, message, exceptionName, startTime, duration, async);
    }

    private void fireEvent(ShareDownloadEventType type, URI uri, FileMetadata fileMetadata, String message, String exceptionName, long startTime, long duration, boolean async)
    {
        ShareDownloadEvent event = createEvent(type);
        event.file = file;
        event.relativePath = relativePath;
        event.uri = uri;
        event.fileMetadata = fileMetadata;
        event.message = message != null ? message(message) : null;
        event.exceptionName = exceptionName;
        event.startTime = startTime;
        event.duration = duration;
        fireEvent(event, async);
    }

    private String message(String message)
    {
        return String.format("%s - %s", message, uri);
    }

    private long now()
    {
        return System.currentTimeMillis();
    }

    public String getURL()
    {
        return uri.toString();
    }

    public FileMetadata getFileMetadata()
    {
        if (fileMetadata == null)
        {
            try
            {
                fileMetadata = newFileMetadata();
                fileMetadata.read();
            }
            catch (IOException e)
            {
                if (logger.isWarnEnabled())
                {
                    logger.warn("not found metedata file[path={}\tmsg={}]", fileMetadata.getPath(), e.getMessage());
                }

                fileMetadata = null;
            }
        }

        return fileMetadata;
    }

    public FileMetadata newFileMetadata()
    {
        return newFileMetadata(createMetaPath());
    }

    public FileMetadata newFileMetadata(Path path)
    {
        return new FileMetadata(path);
    }

    public File getFile()
    {
        return file;
    }

    public int getCount()
    {
        return count.get();
    }

    public long getPosition()
    {
        return position;
    }

    public static ShareDownload get(String url)
    {
        return pool.get(URI.create(url));
    }

    /**
     * 共享下载非首个下载请求的对象,通过此对象发送到后面请求的客户端
     */
    class Transfer
    {
        private String identity;

        private AtomicReference<Path> file = new AtomicReference<>();

        private HttpServletRequest request;

        private AsyncContextWrapper asyncContext;

        private RandomAccessFile in;

        private ServletOutputStream out;

        private long position = 0;

        private byte[] buffer = new byte[BUFFER_SIZE];

        private boolean valid = false;

        private boolean end = false;

        private volatile boolean abort = false;

        private int statusCode = 0;

        private String errorMessage;

        private long startTime;

        private long timeout = 1000 * 60 * 5;

        private AtomicInteger writeTaskNum = new AtomicInteger(0);

        public Transfer(HttpServletRequest request)
        {
            this.request = request;
            this.asyncContext = new AsyncContextWrapper(request.startAsync());
            this.valid = true;
            this.identity = UUIDTool.getUUID();

            asyncContext.setTimeout(timeout);
        }

        private void ensureOpen(Signal signal) throws IOException
        {
            if (!abort && signal != null && file.compareAndSet(null, signal.path))
            {
                HttpServletResponse response = asyncContext.getResponse();
                in = new RandomAccessFile(file.get().toFile(), "r");
                out = response.getOutputStream();
                fileMetadata.writeResponse(response);
                startTime = now();

                FileMetadata.Range range = fileMetadata.getRange(request);
                position = range != null ? range.start : 0;
                in.seek(position);
            }
        }

        private int write0(Signal signal) throws IOException
        {
            if (!isAlive())
            {
                valid = false;
                return -1;
            }

            ensureOpen(signal);

            int length;
            if ((length = in.read(buffer)) != -1)
            {
                out.write(buffer, 0, length);
                position += length;

                writeTrigger();
            }

            asyncContext.resetTimeout();
            return length;
        }

        public void write(Signal signal)
        {
            if (!writing())
            {
                writeTrigger(signal);
            }
        }

        private void writeTrigger()
        {
            writeTrigger(null);
        }

        private void writeTrigger(Signal signal)
        {
            transferExecutor.execute(new WriteTask(signal));
        }

        public void end(Signal signal)
        {
            end = true;
            write(signal);
        }

        public void complete()
        {
            if (!valid)
            {
                return;
            }

            try
            {
                valid = false;

                closeQuietly(in);
                closeQuietly(out);
                asyncContext.complete();

                if (fileMetadata != null && fileMetadata.getContentLength() == getPosition())
                {
                    fireEvent(ShareDownloadEventType.SHARED_DOWNLOAD, fileMetadata, startTime, now() - startTime);
                }
                else
                {
                    fireEvent(ShareDownloadEventType.SHARED_DOWNLOAD_ERROR, fileMetadata, "shared download failed", StreamException.class.getName());
                }
            }
            finally
            {
                if (count.decrementAndGet() <= 0 && file.get() != null)
                {
                    deleteFile(file.get());
                }

                if (logger.isInfoEnabled())
                {
                    logger.info("transfer({}) download completed({}) [uri={}]", identity, getCount(), uri);
                }
            }
        }

        public void recycle()
        {
            asyncContext = null;
            buffer = null;
        }

        public Transfer abort()
        {
            logger.info("transfer({}) client({}) abort [uri={}]", identity, count.decrementAndGet(), uri);

            abort = true;
            transfers.remove(this);
            return this;
        }

        public Transfer terminte(boolean disconnect)
        {
            abort = true;
            file.set(null);
            transfers.remove(this);
            complete();
            if (disconnect) asyncContext.disconnect();
            recycle();
            return this;
        }

        public Transfer terminte()
        {
            if (position > 0)
            {
                logger.info("transfer({}) terminte [uri={}]", identity, uri);

                return terminte(true);
            }
            else
            {
                logger.info("transfer({}) redirect [uri={}]", identity, uri);

                try
                {
                    if (statusCode != 0)
                    {
                        sendError(asyncContext.getResponse(), statusCode, errorMessage);
                    }
                    else
                    {
                        sendRedirect(asyncContext.getResponse(), getURL());
                    }
                }
                catch (IOException e)
                {
                    logger.error(e.getMessage(), e);
                }

                return terminte(false);
            }

        }

        public void sendErrorMessage(int statusCode, String errorMessage)
        {
            this.statusCode = statusCode;
            this.errorMessage = errorMessage;
        }

        public boolean isAlive()
        {
            return !abort && valid;
        }

        public boolean writing()
        {
            return writeTaskNum.get() > 0;
        }

        public long getPosition()
        {
            return position;
        }

        public void setResponseCode(int status)
        {
            try { (asyncContext.getResponse()).setStatus(status); } catch (Exception e) { logger.trace(e.getMessage(), e); }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Transfer transfer = (Transfer) o;

            return identity != null ? identity.equals(transfer.identity) : transfer.identity == null;
        }

        @Override
        public int hashCode()
        {
            return identity != null ? identity.hashCode() : 0;
        }

        class WriteTask implements Runnable
        {
            Signal signal;

            WriteTask(Signal signal)
            {
                this.signal = signal;
                writeTaskNum.incrementAndGet();
            }

            @Override
            public void run()
            {
                writeTaskNum.decrementAndGet();

                if (!isAlive())
                {
                    return;
                }

                synchronized (file)
                {
                    int length = 0;

                    try
                    {
                        length = write0(signal);
                    }
                    catch (Throwable e)
                    {
                        if (isAbortEx(e))
                        {
                            abort();
                        }
                        else
                        {
                            logger.error("transfer({}) write error: {} [uri={}]", identity, e.getMessage(), uri, e);
                            terminte();
                        }
                    }

                    if (end && length == -1)
                    {
                        complete();
                        recycle();
                    }
                }
            }

            private boolean isAbortEx(Throwable e)
            {
                return e instanceof ClientAbortException || (e instanceof IllegalStateException && e.getMessage().contains("STARTING"));
            }
        }
    }

    static class Signal
    {
        protected Path path;

        protected long position;

        Signal(Path path, long position)
        {
            this.path = path;
            this.position = position;
        }
    }

    public static int getTransferSize()
    {
        return transferQueue.size();
    }
}