package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnectionFactory;

public class DummyConnectionManager implements ConnectionManager {

   public DummyConnectionManager() {
   }

   public Object allocateConnection(ManagedConnectionFactory mcf, ConnectionRequestInfo cri) throws ResourceException {
       return null;
   }

   @Override
   public int hashCode() {
      return 42;
   }

   public boolean equals(Object other) {
      if (other == null) {
          return false;
      }
      return getClass().equals(other.getClass());
   }

}
