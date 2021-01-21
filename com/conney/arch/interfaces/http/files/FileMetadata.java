package com.conney.arch.interfaces.http.files;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 文件的元数据类
 */
public class FileMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(FileMetadata.class);

    /**
     * 换行符
     */
    public static final String LINE_BREAK = "\n";

    /**
     * http header content-length name
     */
    public static final String CONTENT_LENGTH_HEADER_NAME = "Content-Length";

    /**
     * http header content-range name
     */
    public static final String CONTENT_RANGE_HEADER_NAME = "Content-Range";

    /**
     * http header content-disposition name
     */
    public static final String CONTENT_DISPOSITION_HEADER_NAME = "Content-Disposition";

    /**
     * http header range name
     */
    public static final String RANGE_HEADER_NAME = "Range";

    /**
     * http header etag name
     */
    public static final String ETAG_HEADER_NAME = "ETag";

    /**
     * 文件所在路径
     */
    private Path path;

    /**
     * header信息数据
     */
    protected Map<String, String> headers = new HashMap<>();

    /**
     * 创建时间
     */
    private long createTime = 0;

    /**
     * 下载所用时间
     */
    private long downloadTime = 0;

    /**
     *　写入本地时间
     */
    private long writeLocalTime = 0;

    public FileMetadata(Path path)
    {
        this.path = path;
    }

    public Integer getContentLength()
    {
        return parseInt(headers.get(CONTENT_LENGTH_HEADER_NAME));
    }

    public String getMd5()
    {
        return headers.get(ETAG_HEADER_NAME);
    }

    public Range getRange(HttpResponse response)
    {
        Header header;
        if ((header = response.getFirstHeader(CONTENT_RANGE_HEADER_NAME)) != null)
        {
            String value = header.getValue().trim();
            if (value.startsWith("bytes"))
            {
                char ch = value.charAt(5);
                value = value.substring((ch == ' ' || ch == '=') ? 6 : 5);
                String[] array = value.split("-|/");
                Range range = new Range();
                range.start = array.length > 0 ? parseLong(array[0]) : -1;
                range.end = array.length > 1 ? parseLong(array[1]) : -1;
                range.length = array.length > 2 ? parseInt(array[2]) : -1;
                return range.validate() ? range : null;
            }
        }
        return null;
    }

    public Range getRange(HttpServletRequest request)
    {
        String value;
        if ((value = request.getHeader(RANGE_HEADER_NAME)) != null)
        {
            value = value.trim();
            if (value.startsWith("bytes"))
            {
                char ch = value.charAt(5);
                value = value.substring((ch == ' ' || ch == '=') ? 6 : 5);
                int index = value.indexOf("-");
                if (index != -1)
                {
                    int length = getContentLength();
                    FileMetadata.Range range = new FileMetadata.Range();
                    range.start = parseLong(value.substring(0, index));
                    range.end = parseLong(value.substring(index + 1), getContentLength());
                    range.end = range.end == -1 ? length : range.end;
                    range.length = length;
                    return range.validate() ? range : null;
                }
            }
        }
        return null;
    }

    /**
     * 读取元数据文件信息
     * 读取文件内容到headers
     */
    public void read() throws FileMetadataNotExistsException
    {
        try
        {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

            for (String line : lines)
            {
                addHeader(line);
            }
        }
        catch (IOException e)
        {
            throw new FileMetadataNotExistsException(e);
        }
    }

    /**
     * 写入元数据到文件
     * 将headers内容写入到文件
     */
    public void write()
    {
        BufferedWriter writer = null;

        try
        {
            writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
            Set<Map.Entry<String, String>> entries = headers.entrySet();

            for (Map.Entry<String, String> entry : entries)
            {
                writer.write(entry.getKey() + ":" + entry.getValue() + LINE_BREAK);
            }
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);
        }
        finally
        {
            IOUtils.closeQuietly(writer);
        }
    }

    private void addHeader(String line)
    {
        if (StringUtils.isNotEmpty(line))
        {
            int index = line.indexOf(":");
            if (index > 1 && index < line.length())
            {
                headers.put(line.substring(0, index), line.substring(index + 1));
            }
        }
    }

    public void putHeader(String name, String value)
    {
        headers.put(name, value);
    }

    /**
     * 读取http返回报文的header信息存储到headers
     */
    public void readResponse(CloseableHttpResponse response, String filename)
    {
        Header[] allHeaders = response.getAllHeaders();
        String name, value;

        headers.put(CONTENT_DISPOSITION_HEADER_NAME, "inline;filename=" + filename);

        for (Header header : allHeaders)
        {
            name = header.getName();
            value = header.getValue();

            if (name.equals(ETAG_HEADER_NAME) && StringUtils.isNotEmpty(value))
            {
                value = value.replace("\"", "");
                if (isMD5(value))
                {
                    headers.put(name, value);
                }
            }
            else
            {
                headers.put(name, value);
            }
        }
    }

    /**
     * 将headers信息写入到http返回报文
     *
     * @param response http返回报文
     */
    public void writeResponse(HttpServletResponse response)
    {
        headers.forEach((name, value) ->
        {
            if (StringUtils.isNotEmpty(value))
            {
                response.addHeader(name, value);
            }
        });

        setCors(response);
    }

    public static void setCors(HttpServletResponse response)
    {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "POST, GET, HEAD");
    }

    public String getHeader(String name)
    {
        return headers.get(name);
    }

    public static String getHeader(CloseableHttpResponse response, String name)
    {
        Header header = response.getLastHeader(name);
        return header != null ? header.getValue() : "";
    }

    public static int getHeaderInt(CloseableHttpResponse response, String name)
    {
        Header header = response.getLastHeader(name);
        return header != null ? parseInt(header.getValue(), -1) : -1;
    }

    public static int parseInt(String value)
    {
        return parseInt(value, 0);
    }

    public static int parseInt(String value, int defaultValue)
    {
        if (StringUtils.isEmpty(value))
        {
            return defaultValue;
        }

        try
        {
            return StringUtils.isNotEmpty(value) ? Integer.parseInt(value) : defaultValue;
        }
        catch (NumberFormatException e)
        {
            return defaultValue;
        }
    }

    public static long parseLong(String value)
    {
        return parseLong(value, 0);
    }

    public static long parseLong(String value, long defaultValue)
    {
        if (StringUtils.isEmpty(value))
        {
            return defaultValue;
        }

        try
        {
            return StringUtils.isNotEmpty(value) ? Long.parseLong(value) : defaultValue;
        }
        catch (NumberFormatException e)
        {
            return defaultValue;
        }
    }

    private boolean isMD5(String s)
    {
        return s != null && s.length() == 32;
    }

    public Path getPath()
    {
        return path;
    }

    public void setPath(Path path)
    {
        this.path = path;
    }

    public long getCreateTime()
    {
        return createTime;
    }

    public void setCreateTime(long createTime)
    {
        this.createTime = createTime;
    }

    public long getDownloadTime()
    {
        return downloadTime;
    }

    public void incrDownloadTime(long downloadTime)
    {
        this.downloadTime += downloadTime;
    }

    public long getWriteLocalTime()
    {
        return writeLocalTime;
    }

    public void incrWriteLocalTime(long writeLocalTime)
    {
        this.writeLocalTime += writeLocalTime;
    }

    protected static class Range
    {
        public long start = -1;
        public long end = -1;
        public long length = -1;

        public boolean validate()
        {
            if (end >= length)
            {
                end = length - 1;
            }
            return (start >= 0) && (end >= 0) && (start <= end) && (length > 0);
        }

        @Override
        public String toString()
        {
            return String.format("bytes %d-%d/%d", start, end > 0 && end < length ? end : length, length);
        }
    }
}
