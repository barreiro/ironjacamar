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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ManagedConnectionPoolBenchmark {

    private static final String JNDI_PREFIX = "java:/eis/";

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        String name = UUID.randomUUID().toString();
        Embedded embedded;
        ResourceAdapterArchive raa;

        @Setup
        public void setupEmbedded() throws Throwable {
            embedded = EmbeddedFactory.create(false);
            embedded.startup();

            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-naming.xml"));
            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-transaction.xml"));
            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-stdio.xml"));
            embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-jca.xml"));
            embedded.deploy(createRaa(name));

            InputStream is = ManagedConnectionPoolBenchmark.class.getClassLoader().getResourceAsStream("logging.properties");
            LogManager.getLogManager().readConfiguration(is);
        }

        @TearDown
        public void tearDownEmbedded() throws Throwable {

            LogManager.getLogManager().readConfiguration();

            embedded.undeploy(raa);

            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-jca.xml"));
            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-stdio.xml"));
            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-transaction.xml"));
            embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-naming.xml"));

            embedded.shutdown();
            embedded = null;

            name = UUID.randomUUID().toString();
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
        Context context;

        UserTransaction ut;

        DummyConnectionFactory dcf;

        @Setup
        public void setupContext(BenchmarkState state) throws Throwable {
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

            DummyConnection tc = state.dcf.getConnection();

            // Wait some time
            Thread.sleep(100);

            // So something
            BlackHole.consumeCPU(1000 * 1000);

            tc.callMe();

            tc.close();

            if (state.ut != null) {
                state.ut.commit();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}
