package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;

import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

public class DummyManagedConnection implements ManagedConnection {

   private PrintWriter logwriter;

   private static Logger log = Logger.getLogger("DummyManagedConnection");

   private DummyManagedConnectionFactory mcf;

   private List<ConnectionEventListener> listeners;

   private DummyConnectionImpl connection;

   public DummyManagedConnection(DummyManagedConnectionFactory mcf) {
      this.mcf = mcf;
      this.logwriter = null;
      this.listeners = Collections.synchronizedList(new ArrayList<ConnectionEventListener>(1));
      this.connection = null;
   }

   public Object getConnection(Subject subject, ConnectionRequestInfo cxRequestInfo) throws ResourceException {
      log.finest("getConnection()");
      connection = new DummyConnectionImpl(this, mcf);
      return connection;
   }

   public void associateConnection(Object connection) throws ResourceException {
      log.finest("associateConnection()");

      if (connection == null) {
          throw new ResourceException("Null connection handle");
      }
      if (!(connection instanceof DummyConnectionImpl)) {
          throw new ResourceException("Wrong connection handle");
      }
      this.connection = (DummyConnectionImpl)connection;
   }

   public void cleanup() throws ResourceException {
      log.finest("cleanup()");
   }

   public void destroy() throws ResourceException {
      log.finest("destroy()");
   }

   public void addConnectionEventListener(ConnectionEventListener listener) {
      log.finest("addConnectionEventListener()");
      if (listener == null) {
          throw new IllegalArgumentException("Listener is null");
      }
      listeners.add(listener);
   }

   public void removeConnectionEventListener(ConnectionEventListener listener) {
      log.finest("removeConnectionEventListener()");
      if (listener == null) {
          throw new IllegalArgumentException("Listener is null");
      }
      listeners.remove(listener);
   }

   void closeHandle(DummyConnection handle) {
      ConnectionEvent event = new ConnectionEvent(this, ConnectionEvent.CONNECTION_CLOSED);
      event.setConnectionHandle(handle);
      for (ConnectionEventListener cel : listeners) {
         cel.connectionClosed(event);
      }
   }

   public PrintWriter getLogWriter() throws ResourceException {
      log.finest("getLogWriter()");
      return logwriter;
   }

   public void setLogWriter(PrintWriter out) throws ResourceException {
      log.finest("setLogWriter()");
      logwriter = out;
   }

   public LocalTransaction getLocalTransaction() throws ResourceException {
      throw new NotSupportedException("LocalTransaction not supported");
   }

   public XAResource getXAResource() throws ResourceException {
      throw new NotSupportedException("GetXAResource not supported not supported");
   }

   public ManagedConnectionMetaData getMetaData() throws ResourceException {
      log.finest("getMetaData()");
      return new DummyManagedConnectionMetaData();
   }

   void callMe() {
      log.finest("callMe()");
   }

}
