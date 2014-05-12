package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import javax.naming.NamingException;
import javax.naming.Reference;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;

public class DummyConnectionFactoryImpl implements DummyConnectionFactory {

   private static final long serialVersionUID = 1L;

   private Reference reference;

   private DummyManagedConnectionFactory mcf;

   private ConnectionManager connectionManager;

   public DummyConnectionFactoryImpl() {
   }

   public DummyConnectionFactoryImpl(DummyManagedConnectionFactory mcf, ConnectionManager cxManager)  {
      this.mcf = mcf;
      this.connectionManager = cxManager;
   }

   @Override
   public DummyConnection getConnection() throws ResourceException {
      return (DummyConnection) connectionManager.allocateConnection(mcf, null);
   }

   @Override
   public Reference getReference() throws NamingException {
      return reference;
   }

   @Override
   public void setReference(Reference reference) {
      this.reference = reference;
   }

}
