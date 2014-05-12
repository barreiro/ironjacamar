package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import javax.resource.ResourceException;

import javax.resource.spi.ManagedConnectionMetaData;

public class DummyManagedConnectionMetaData implements ManagedConnectionMetaData {

   public DummyManagedConnectionMetaData() {
   }

   /**
    * Returns Product name of the underlying EIS instance connected through the ManagedConnection.
    */
   @Override
   public String getEISProductName() throws ResourceException {
      return null; //TODO
   }

   /**
    * Returns Product version of the underlying EIS instance connected through the ManagedConnection.
    */
   @Override
   public String getEISProductVersion() throws ResourceException {
      return null; //TODO
   }

   /**
    * Returns maximum limit on number of active concurrent connections
    */
   @Override
   public int getMaxConnections() throws ResourceException {
      return 0; //TODO
   }

   /**
    * Returns name of the user associated with the ManagedConnection instance
    */
   @Override
   public String getUserName() throws ResourceException {
      return null; //TODO
   }

}
