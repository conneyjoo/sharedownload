package com.conney.arch.interfaces.http.files;

import com.conney.arch.utils.ReflectUtils;
import org.apache.catalina.connector.Request;
import org.apache.catalina.core.AsyncContextImpl;
import org.apache.coyote.ActionHook;
import org.apache.coyote.AsyncStateMachine;
import org.apache.tomcat.util.net.NioChannel;
import org.apache.tomcat.util.net.SocketWrapperBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;

public class AsyncContextWrapper implements AsyncContext
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncContextWrapper.class);

    private final AsyncContextImpl asyncContext;

    private final AsyncStateMachine asyncStateMachine;

    private final SocketWrapperBase<?> socketWrapper;

    private final Field lastAsyncStart;

    public AsyncContextWrapper(AsyncContext asyncContext)
    {
        this.asyncContext = (AsyncContextImpl) asyncContext;
        Request res = (Request) ReflectUtils.forceGet(asyncContext, "request");
        org.apache.coyote.Request coyoteRequest = res.getCoyoteRequest();
        ActionHook hook = (ActionHook) ReflectUtils.forceGet(coyoteRequest, "hook");
        this.asyncStateMachine = (AsyncStateMachine) ReflectUtils.forceGet(hook, "asyncStateMachine");
        this.socketWrapper = (SocketWrapperBase) ReflectUtils.forceGet(hook, "socketWrapper");
        this.lastAsyncStart = ReflectUtils.getDeclaredField(asyncStateMachine, "lastAsyncStart");
        this.lastAsyncStart.setAccessible(true);
    }

    @Override
    public ServletRequest getRequest()
    {
        return asyncContext.getRequest();
    }

    @Override
    public HttpServletResponse getResponse()
    {
        try
        {
            return (HttpServletResponse) asyncContext.getResponse();
        }
        catch (Exception e)
        {
            return null;
        }
    }

    @Override
    public boolean hasOriginalRequestAndResponse()
    {
        return asyncContext.hasOriginalRequestAndResponse();
    }

    @Override
    public void dispatch()
    {
        asyncContext.dispatch();
    }

    @Override
    public void dispatch(String path)
    {
        asyncContext.dispatch(path);
    }

    @Override
    public void dispatch(ServletContext context, String path)
    {
        asyncContext.dispatch(context, path);
    }

    @Override
    public void complete()
    {
        if (asyncContext != null)
        {
            try
            {
                asyncContext.complete();
            }
            catch (Exception e)
            {
                logger.warn(e.getMessage());
            }
        }
    }

    @Override
    public void start(Runnable run)
    {
        asyncContext.start(run);
    }

    @Override
    public void addListener(AsyncListener listener)
    {
        asyncContext.addListener(listener);
    }

    @Override
    public void addListener(AsyncListener listener, ServletRequest request, ServletResponse response)
    {
        asyncContext.addListener(listener, request, response);
    }

    @Override
    public <T extends AsyncListener> T createListener(Class<T> clazz) throws ServletException
    {
        return asyncContext.createListener(clazz);
    }

    public void disconnect()
    {
        socketWrapper.getEndpoint().getExecutor().execute(() ->
        {
            try
            {
                ((NioChannel) socketWrapper.getSocket()).close(true);
            }
            catch (IOException e)
            {
            }
        });
    }

    @Override
    public void setTimeout(long timeout)
    {
        asyncContext.setTimeout(timeout);
    }

    @Override
    public long getTimeout()
    {
        return asyncContext.getTimeout();
    }

    public void resetTimeout()
    {
        try
        {
            lastAsyncStart.set(asyncStateMachine, System.currentTimeMillis());
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        }
    }
}
