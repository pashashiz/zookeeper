package com.ps;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ZookeeperTest {

    private ZooKeeper zk;

    @Before
    public void connect() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper("localhost", 2181, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected)
                connected.countDown();
            else
                throw new RuntimeException("Did not connect: " + event);
        });
        connected.await();
        ZKUtil.deleteRecursive(zk, "/test");
        zk.create("/test", new byte[0], OPEN_ACL_UNSAFE, PERSISTENT);
    }

    @After
    public void close() throws Exception {
        zk.close();
    }

    @Test
    public void CRUD() throws Exception {
        zk.create("/test/hello", "hello pavlo".getBytes(), OPEN_ACL_UNSAFE, PERSISTENT);
        Stat stat = zk.exists("/test/hello", false);
        assertThat("created node should exist", stat, notNullValue());
        byte[] data1 = zk.getData("/test/hello", false, stat);
        assertThat("node data should be created", new String(data1), is("hello pavlo"));
        zk.setData("/test/hello", "hello randy".getBytes(), stat.getVersion());
        byte[] data2 = zk.getData("/test/hello", false, stat);
        assertThat("node data should be modified", new String(data2), is("hello randy"));
        zk.delete("/test/hello", zk.exists("/test/hello", false).getVersion());
        assertThat("node should be deleted", zk.exists("/test/hello", false), nullValue());
    }

    @Test
    public void watcher() throws Exception {
        CountDownLatch changed = new CountDownLatch(1);
        zk.register(event -> {
            assertThat("watched node should be updated", event.getPath(), is("/test/watched"));
            changed.countDown();
        });
        zk.create("/test/watched", "data".getBytes(), OPEN_ACL_UNSAFE, PERSISTENT);
        Stat stat = zk.exists("/test/watched", true);
        zk.setData("/test/watched", "updated".getBytes(), stat.getVersion());
        assertThat("watched node should be updated",
                changed.await(5, TimeUnit.SECONDS), is(true));
    }
}