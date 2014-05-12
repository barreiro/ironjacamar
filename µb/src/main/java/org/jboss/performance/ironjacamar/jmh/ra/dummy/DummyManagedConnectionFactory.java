package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Set;

import java.util.logging.Logger;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;

import javax.security.auth.Subject;

public class DummyManagedConnectionFactory implements ManagedConnectionFactory, ResourceAdapterAssociation {

   private static Logger log = Logger.getLogger("TestManagedConnectionFactory");

   private ResourceAdapter ra;

   private PrintWriter logwriter;

   public DummyManagedConnectionFactory() {
   }

   public Object createConnectionFactory(ConnectionManager cxManager) throws ResourceException {
      log.finest("createConnectionFactory()");
      return new DummyConnectionFactoryImpl(this, cxManager);
   }

   public Object createConnectionFactory() throws ResourceException {
      throw new ResourceException("This resource adapter doesn't support non-managed environments");
   }

   public ManagedConnection createManagedConnection(Subject subject, ConnectionRequestInfo cxRequestInfo) throws ResourceException {
      log.finest("createManagedConnection()");
      return new DummyManagedConnection(this);
   }

   public ManagedConnection matchManagedConnections(Set connectionSet, Subject subject, ConnectionRequestInfo cxRequestInfo) throws ResourceException  {
      log.finest("matchManagedConnections()");
      ManagedConnection result = null;
      Iterator it = connectionSet.iterator();
      while (result == null && it.hasNext()) {
         ManagedConnection mc = (ManagedConnection)it.next();
         if (mc instanceof DummyManagedConnection) {
            result = mc;
         }
      }
      return result;
   }

   public PrintWriter getLogWriter() throws ResourceException {
      log.finest("getLogWriter()");
      return logwriter;
   }

   public void setLogWriter(PrintWriter out) throws ResourceException {
      log.finest("setLogWriter()");
      logwriter = out;
   }

   public ResourceAdapter getResourceAdapter() {
      log.finest("getResourceAdapter()");
      return ra;
   }


   public void setResourceAdapter(ResourceAdapter ra) {
      log.finest("setResourceAdapter()");
      this.ra = ra;
   }

   @Override
   public int hashCode() {
      int result = 17;
      return result;
   }

   @Override
   public boolean equals(Object other) {
      if (other == null) {
         return false;
      }
      if (other == this) {
         return true;
      }
      if (!(other instanceof DummyManagedConnectionFactory)) {
          return false;
      }
      DummyManagedConnectionFactory obj = (DummyManagedConnectionFactory)other;
      boolean result = true; 
      return result;
   }

}
