package org.jboss.performance.ironjacamar.jmh.ra.dummy;

public interface DummyConnection {

    public void doWork(boolean b, long amount);

    public void doSleep(boolean b, long amount);

    public void doYeld(boolean b);

    public void close();
}
