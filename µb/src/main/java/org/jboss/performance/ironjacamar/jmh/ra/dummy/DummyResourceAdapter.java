package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import java.io.Serializable;
import java.util.logging.Logger;

import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;

import javax.transaction.xa.XAResource;

public class DummyResourceAdapter implements ResourceAdapter, Serializable {

   private static final long serialVersionUID = 1L;

   /** The logger */
   private static Logger log = Logger.getLogger("TestResourceAdapter");

   public DummyResourceAdapter() {
   }

   public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) throws ResourceException {
      log.finest("endpointActivation()");
   }

   public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) {
      log.finest("endpointDeactivation()");
   }

   public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {
      log.finest("start()");
   }

   public void stop() {
      log.finest("stop()");
   }

   public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException {
      log.finest("getXAResources()");
      return null;
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
      if (!(other instanceof DummyResourceAdapter)) {
          return false;
      }
      DummyResourceAdapter obj = (DummyResourceAdapter)other;
      boolean result = true; 
      return result;
   }

}
