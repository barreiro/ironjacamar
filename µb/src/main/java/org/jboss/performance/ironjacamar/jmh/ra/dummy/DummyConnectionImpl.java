package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import org.openjdk.jmh.logic.BlackHole;

public class DummyConnectionImpl implements DummyConnection {

   private DummyManagedConnection mc;

   private DummyManagedConnectionFactory mcf;

   public DummyConnectionImpl(DummyManagedConnection mc, DummyManagedConnectionFactory mcf) {
      this.mc = mc;
      this.mcf = mcf;
   }

   @Override
   public void doWork(boolean b, long amount) {
      if (b) {
         BlackHole.consumeCPU(amount);
         mc.callMe();
      }
   }

   @Override
   public void doSleep(boolean b, long amount) {
      if (b) {
         try {
            Thread.sleep(amount);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         mc.callMe();
      }
   }

   @Override
   public void doYeld(boolean b) {
      if (b) {
         Thread.yield();
         mc.callMe();
      }
   }

   public void close() {
      mc.closeHandle(this);
   }

}
