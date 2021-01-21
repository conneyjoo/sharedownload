package com.conney.arch.interfaces.http.files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class FileAssist
{
    private static final Logger logger = LoggerFactory.getLogger(FileAssist.class);

    /**
     * 计算文件md5算法的名称
     */
    private static final String MD5_ALGORITHM_NAME = "MD5";

    /**
     * 转换16进制所需的字符数组
     */
    private static final char[] HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * 临时文件后缀名
     */
    public static final String TEMP_SUFFIX = ".tmp";

    public static String md5Sum(RandomAccessFile in) throws IOException
    {
        in.seek(0);
        MessageDigest messageDigest = getDigest(MD5_ALGORITHM_NAME);
        final byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1)
        {
            messageDigest.update(buffer, 0, bytesRead);
        }
        return new String(encodeHex(messageDigest.digest()));
    }

    public static MessageDigest getDigest(String algorithm)
    {
        try
        {
            return MessageDigest.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException ex)
        {
            throw new IllegalStateException("Could not find MessageDigest with algorithm \"" + algorithm + "\"", ex);
        }
    }

    public static char[] encodeHex(byte[] bytes)
    {
        char[] chars = new char[32];
        for (int i = 0; i < chars.length; i = i + 2)
        {
            byte b = bytes[i / 2];
            chars[i] = HEX_CHARS[(b >>> 0x4) & 0xf];
            chars[i + 1] = HEX_CHARS[b & 0xf];
        }
        return chars;
    }

    /**
     * 创建文件(支持目录不存在)
     *
     * @param path 文件路径
     * @return 文件路径
     * @throws IOException
     */
    public static Path createFile(Path path) throws IOException
    {
        if (Files.exists(path))
        {
            return path;
        }
        else
        {

            if (!Files.exists(path.getParent()))
            {
                Files.createDirectories(path.getParent());
            }

            return Files.createFile(path);
        }
    }

    /**
     * 创建临时文件(支持目录不存在)
     *
     * @param path 文件路径
     * @return 文件路径
     * @throws IOException
     */
    public static Path createTempFile(Path path) throws IOException
    {
        if (Files.exists(path))
        {
            return path;
        }
        else
        {

            if (!Files.exists(path.getParent()))
            {
                Files.createDirectories(path.getParent());
            }

            return Files.createTempFile(path.getParent(), path.toFile().getName(), TEMP_SUFFIX);
        }
    }

    /**
     * 删除文件
     *
     * @param path 文件路径
     * @return boolean(文件存在:true, 不存在:false)
     */
    public static boolean deleteFile(Path path)
    {
        logger.info("delete file[{}]", path);

        try
        {
            return Files.deleteIfExists(path);
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * 复制文件
     *
     * @param source 源文件
     * @param target 目标文件
     */
    public static void copy(File source, File target) throws IOException
    {
        FileCopyUtils.copy(source, target);
    }

    /**
     * 文件重命名
     *
     * @param src  原文件
     * @param dest 目标文件
     */
    public static void rename(File src, File dest)
    {
        if (logger.isInfoEnabled())
        {
            logger.info("rename file[src={}\tdest={}]", src, dest);
        }

        src.renameTo(dest);
    }
}
