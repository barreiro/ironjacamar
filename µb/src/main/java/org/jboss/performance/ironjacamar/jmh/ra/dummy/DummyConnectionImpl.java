package org.jboss.performance.ironjacamar.jmh.ra.dummy;

public class DummyConnectionImpl implements DummyConnection {

   private DummyManagedConnection mc;

   private DummyManagedConnectionFactory mcf;

   public DummyConnectionImpl(DummyManagedConnection mc, DummyManagedConnectionFactory mcf) {
      this.mc = mc;
      this.mcf = mcf;
   }

   public void callMe() {
      mc.callMe();
   }

   public void close() {
      mc.closeHandle(this);
   }

}
