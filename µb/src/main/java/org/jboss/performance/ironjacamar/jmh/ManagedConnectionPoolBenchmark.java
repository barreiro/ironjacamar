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
import org.openjdk.jmh.logic.results.RunResult;
import org.openjdk.jmh.output.format.TextReportFormat;
import org.openjdk.jmh.output.results.ResultFormat;
import org.openjdk.jmh.output.results.ResultFormatFactory;
import org.openjdk.jmh.runner.BenchmarkRecord;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.parameters.Defaults;
import org.openjdk.jmh.runner.parameters.TimeValue;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.UserTransaction;
import java.util.*;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ManagedConnectionPoolBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private static final Random RANDOM = new Random();
        private static final String NAME = UUID.randomUUID().toString();
        private static volatile Embedded embedded = null;
        private static ResourceAdapterArchive raa = createRaa(NAME);

        @Setup
        public static synchronized void setupEmbedded() throws Throwable {
            if (embedded == null) {
                embedded = EmbeddedFactory.create(false);
                embedded.startup();
                embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-naming.xml"));
                embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-transaction.xml"));
                embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-stdio.xml"));
                embedded.deploy(Thread.currentThread().getContextClassLoader().getResource("µb-jca.xml"));
                embedded.deploy(raa);
            }
        }

        @TearDown
        public static synchronized void tearDownEmbedded() throws Throwable {
            if (embedded != null) {
                embedded.undeploy(raa);
                embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-jca.xml"));
                embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-stdio.xml"));
                embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-transaction.xml"));
                embedded.undeploy(Thread.currentThread().getContextClassLoader().getResource("µb-naming.xml"));
                embedded.shutdown();
                embedded = null;
            }
        }

        private static ResourceAdapterArchive createRaa(String name) {
            JavaArchive ja = ShrinkWrap.create(JavaArchive.class, UUID.randomUUID().toString() + ".jar");
            ja.addPackage(DummyConnection.class.getPackage());

            raa = ShrinkWrap.create(ResourceAdapterArchive.class, name + ".rar");
            raa.addAsLibrary(ja);
            raa.addAsManifestResource("µb-dummy-ra.xml", "ra.xml");
            raa.addAsManifestResource("µb-ironjacamar.xml", "ironjacamar.xml");
            return raa;
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        Random random;

        Context context;

        UserTransaction transaction;

        DummyConnectionFactory dummy;

        @Setup
        public void setupContext(BenchmarkState state) throws Throwable {
            random = new Random(state.RANDOM.nextLong());
            context = new InitialContext();
            transaction = (UserTransaction) context.lookup("java:/UserTransaction");
            dummy = (DummyConnectionFactory) context.lookup("java:/eis/dummy");
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
             if (state.transaction != null) {
                state.transaction.begin();
             }

             DummyConnection dc = state.dummy.getConnection();

             // Do some work
             dc.doWork(false, state.random.nextInt());

             // Yeld!
             dc.doYeld(false);

             // Wait some time (5ms average)
             dc.doSleep(true, state.random.nextInt(10));

             // Do some work
             dc.doWork(true, state.random.nextInt(1000 * 1000));

             dc.close();

             if (state.transaction != null) {
                 state.transaction.commit();
             }
        } catch (Throwable t) {
             // t.printStackTrace();
             try {
                 if (state.transaction != null) {
                     state.transaction.rollback();
                 }
             } catch (Throwable tr) {
                 System.out.println("tr.getMessage() = " + tr.getMessage());
             }
        }
    }

    // For running from IDE
    public static void main(String[] args) throws RunnerException {
        Options baseOpts = new OptionsBuilder()
                .mode(Mode.Throughput)
                .mode(Mode.AverageTime)
                .warmupTime(TimeValue.seconds(1))
                .measurementTime(TimeValue.seconds(10))
                .warmupIterations(10)
                .measurementIterations(10)
                .forks(1)
                .threads(100)
                .verbosity(VerboseMode.NORMAL)
                .build();

        Options salMCP = new OptionsBuilder()
                .parent(baseOpts)
                .jvmArgs("-Dironjacamar.mcp=org.jboss.jca.core.connectionmanager.pool.mcp.SemaphoreArrayListManagedConnectionPool")
                .build();

        Map<BenchmarkRecord, RunResult> resultsSAL = new Runner(salMCP).run();

        Options sclqMCP = new OptionsBuilder()
                .parent(baseOpts)
                .jvmArgs("-Dironjacamar.mcp=org.jboss.jca.core.connectionmanager.pool.mcp.SemaphoreConcurrentLinkedQueueManagedConnectionPool")
                .build();

        Map<BenchmarkRecord, RunResult> resultsSCLQ = new Runner(sclqMCP).run();

        System.out.println("------------------");
        System.out.println("\nSemaphoreArrayList");
        new TextReportFormat(System.out, VerboseMode.NORMAL).endRun(resultsSAL);
        System.out.println("\nSemaphoreConcurrentLinkedQueue");
        new TextReportFormat(System.out, VerboseMode.NORMAL).endRun(resultsSAL);
    }

}
