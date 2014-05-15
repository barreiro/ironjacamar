package org.jboss.performance.ironjacamar.jmh;

import org.jboss.jca.embedded.Embedded;
import org.jboss.jca.embedded.EmbeddedFactory;

import org.jboss.performance.ironjacamar.jmh.ra.dummy.DummyConnection;
import org.jboss.performance.ironjacamar.jmh.ra.dummy.DummyConnectionFactory;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.logic.BlackHole;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.UserTransaction;
import java.io.InputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ManagedConnectionPoolBenchmark {

    private static final String JNDI_PREFIX = "java:/eis/";

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        Random random = new Random();
        String name;
        Embedded embedded;
        ResourceAdapterArchive raa;

        @Setup
        public void setupEmbedded() throws Throwable {
            name = UUID.randomUUID().toString();

            embedded = EmbeddedFactory.create(false);
            embedded.startup();
            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-naming.xml"));
            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-transaction.xml"));
            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-stdio.xml"));
            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-jca.xml"));
            embedded.deploy(createRaa(name));
        }

        @TearDown
        public void tearDownEmbedded() throws Throwable {
            embedded.undeploy(raa);
            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-jca.xml"));
            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-stdio.xml"));
            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-transaction.xml"));
            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-naming.xml"));
            embedded.shutdown();
            embedded = null;
        }

        private ResourceAdapterArchive createRaa(String name) throws Throwable {
            JavaArchive ja = ShrinkWrap.create(JavaArchive.class, UUID.randomUUID().toString() + ".jar");
            ja.addPackage(DummyConnection.class.getPackage());

            raa = ShrinkWrap.create(ResourceAdapterArchive.class, name + ".rar");
            raa.addAsLibrary(ja);
            raa.addAsManifestResource("dummy-ra.xml", "ra.xml");
            return raa;
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        Random random;

        Context context;

        UserTransaction ut;

        DummyConnectionFactory dcf;

        @Setup
        public void setupContext(BenchmarkState state) throws Throwable {
            random = new Random(state.random.nextLong());
            context = new InitialContext();
            ut = (UserTransaction) context.lookup("java:/UserTransaction");
            dcf = (DummyConnectionFactory) context.lookup(JNDI_PREFIX + state.name);
        }

        @TearDown
        public void tearDownContext() {
            if (context != null) {
                try {
                    context.close();
                } catch (NamingException ne) {
                    // Ignore
                }
                context = null;
            }
        }
    }

    @GenerateMicroBenchmark
    @Group
    public void testMethod(ThreadState state) {
         try{
             if (state.ut != null) {
                state.ut.begin();
             }

             DummyConnection dc = state.dcf.getConnection();

             // Do some work
             BlackHole.consumeCPU(state.random.nextInt());

             // Yeld !
             Thread.yield();

             // Wait some time
             //Thread.sleep(state.random.nextInt(10));

             // Do some work
             BlackHole.consumeCPU(state.random.nextInt());

             dc.callMe();

             dc.close();

             if (state.ut != null) {
                 state.ut.commit();
             }
        } catch (Throwable t) {
             t.printStackTrace();
             try {
                 if (state.ut != null) {
                     state.ut.rollback();
                 }
             } catch (Throwable tr) {
                 tr.printStackTrace();
             }
        }
    }

}
