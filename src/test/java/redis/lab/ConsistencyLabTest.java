package redis.lab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import redis.lab.ConsistencyLab.Outcome;
import redis.lab.Linearizability.Verdict;
import redis.lab.Workload.Endpoint;
import redis.lab.Workload.Settings;
import redis.protocol.RespClient;
import redis.server.Config;
import redis.server.RedisServer;

/**
 * The whole chain end to end: real servers, concurrent clients, recorded
 * histories, the checker. With 50 ms of simulated replication lag the
 * default replica reads must be caught out, and linearizable mode must not.
 */
class ConsistencyLabTest {

    @Test
    void labSeparatesStaleReplicaReadsFromLinearizableOnes() throws Exception {
        try (RedisServer master = new RedisServer(Config.defaults().withPort(0).withSimulatedReplicationLag(50))) {
            master.start();
            try (RedisServer replica = new RedisServer(Config.defaults().withPort(0).withMaster("127.0.0.1", master.port()))) {
                replica.start();
                awaitConnected(replica.port());

                List<Outcome> outcomes = ConsistencyLab.runAll(
                        new Endpoint("127.0.0.1", master.port()),
                        new Endpoint("127.0.0.1", replica.port()),
                        new Settings(4, 40, 2, 1, 1));

                Outcome onMaster = outcomes.get(0);
                Outcome eventual = outcomes.get(1);
                Outcome linearizable = outcomes.get(2);

                assertEquals(Verdict.LINEARIZABLE, onMaster.result().verdict(), onMaster.result().explanation());
                assertEquals(Verdict.NOT_LINEARIZABLE, eventual.result().verdict());
                assertFalse(eventual.result().anomalies().isEmpty());
                assertEquals(Verdict.LINEARIZABLE, linearizable.result().verdict(), linearizable.result().explanation());
                assertEquals(0, linearizable.run().failedReads(), "no read should have been refused");
                assertTrue(linearizable.note().contains("requests to the master"), linearizable.note());
            }
        }
    }

    private static void awaitConnected(int replicaPort) throws Exception {
        try (RespClient c = new RespClient(replicaPort)) {
            for (int i = 0; i < 200; i++) {
                if ("connected".equals(((List<?>) c.call("ROLE")).get(3))) {
                    return;
                }
                Thread.sleep(20);
            }
        }
        throw new AssertionError("replica never connected");
    }
}
