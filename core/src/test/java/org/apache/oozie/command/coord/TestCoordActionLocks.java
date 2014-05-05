package org.apache.oozie.command.coord;

import junit.framework.Assert;
import org.apache.hcatalog.data.Pair;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XLog;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *  Test cases to check if coord action locks aren't coarse.
 */
public class TestCoordActionLocks extends XDataTestCase {
    protected Services services;

    private ConcurrentMap<Thread, Pair<Long, Long>> threadStartEndTimes =
            new ConcurrentHashMap<Thread, Pair<Long, Long>>();
    private CallableQueueService queueService;
    private XLog log = XLog.getLog(getClass());

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        services = new Services();
        services.init();
        queueService = services.get(CallableQueueService.class);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordActionInputCheckXCommandLocks() throws Exception {
        String jobId = "0000000-100-TestCoordActionInputLocks-C";
        MyCoordActionInputCheckXCommand callable1 = new MyCoordActionInputCheckXCommand(jobId + "@1", jobId);
        MyCoordActionInputCheckXCommand callable2 = new MyCoordActionInputCheckXCommand(jobId + "@2", jobId);
        queueService.queue(callable1);
        queueService.queue(callable2);
        while (callable1.end > 0 && callable2.end > 0) {
            Thread.sleep(100);
        }
        if (callable1.end < callable2.start || callable2.end < callable1.start) {
            log.debug("Callable1::start {0}, Callable1::end {1}, Callable2::start {2},Callable2::end {3}",
                            callable1.start, callable1.end, callable2.start, callable2.end);
            Assert.fail("Two coord input check actions didn't run concurrently");
        }
    }

    public void testCoordActionUdpateXCommandLocks() throws Exception {
        String jobId = "0000000-100-TestCoordActionUpdateLocks-C";
        WorkflowJobBean workflow1 = new WorkflowJobBean();
        workflow1.setParentId(jobId + "@1");
        WorkflowJobBean workflow2 = new WorkflowJobBean();
        workflow2.setParentId(jobId + "@2");
        MyCoordActionUpdateXCommand callable1 = new MyCoordActionUpdateXCommand(workflow1);
        MyCoordActionUpdateXCommand callable2 = new MyCoordActionUpdateXCommand(workflow2);
        queueService.queue(callable1);
        queueService.queue(callable2);
        while (callable1.end > 0 && callable2.end > 0) {
            Thread.sleep(100);
        }
        if (callable1.end < callable2.start || callable2.end < callable1.start) {
            log.debug("Callable1::start {0}, Callable1::end {1}, Callable2::start {2},Callable2::end {3}",
                            callable1.start, callable1.end, callable2.start, callable2.end);
            Assert.fail("Two coord update actions didn't run concurrently");
        }
    }

    public void testCoordActionReadyXCommandLocks() throws Exception {
        String jobId = "0000000-100-TestCoordActionReadyLocks-C";
        MyCoordActionReadyXCommand callable1 = new MyCoordActionReadyXCommand(jobId);
        MyCoordActionReadyXCommand callable2 = new MyCoordActionReadyXCommand(jobId);
        queueService.queue(callable1);
        queueService.queue(callable2);
        while (callable1.end > 0 && callable2.end > 0) {
            Thread.sleep(100);
        }
        if (callable1.end < callable2.start || callable2.end < callable1.start) {
            log.debug("Callable1::start {0}, Callable1::end {1}, Callable2::start {2},Callable2::end {3}",
                            callable1.start, callable1.end, callable2.start, callable2.end);
            Assert.fail("Two coord ready actions didn't run concurrently");
        }
    }

    public void testCoordActionStartXCommandLocks() throws Exception {
        String jobId = "0000000-100-TestCoordActionReadyLocks-C";
        MyCoordActionStartXCommand callable1 = new MyCoordActionStartXCommand(jobId + "@1", jobId);
        MyCoordActionStartXCommand callable2 = new MyCoordActionStartXCommand(jobId + "@2", jobId);
        queueService.queue(callable1);
        queueService.queue(callable2);
        while (callable1.end > 0 && callable2.end > 0) {
            Thread.sleep(100);
        }
        if (callable1.end < callable2.start || callable2.end < callable1.start) {
            log.debug("Callable1::start {0}, Callable1::end {1}, Callable2::start {2},Callable2::end {3}",
                            callable1.start, callable1.end, callable2.start, callable2.end);
            Assert.fail("Two coord start actions didn't run concurrently");
        }
    }

    private class MyCoordActionInputCheckXCommand extends CoordActionInputCheckXCommand {

        private long start;
        private long end;

        public MyCoordActionInputCheckXCommand(String argActionId, String argJobId) {
            super(argActionId, argJobId);
        }

        @Override
        protected void loadState() throws CommandException {
        }

        @Override
        protected void executeInterrupts() {
        }

        @Override
        protected void verifyPrecondition() throws CommandException, PreconditionException {
        }

        @Override
        protected Void execute() throws CommandException {
            start = System.currentTimeMillis();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {  }
            end = System.currentTimeMillis();
            return null;
        }
    }

    private class MyCoordActionStartXCommand extends CoordActionStartXCommand {

        private long start;
        private long end;

        public MyCoordActionStartXCommand(String argActionId, String argJobId) {
            super(argActionId, "none", "none", argJobId);
        }

        @Override
        protected void loadState() throws CommandException {
        }

        @Override
        protected void executeInterrupts() {
        }

        @Override
        protected void verifyPrecondition() throws PreconditionException {
        }

        @Override
        protected Void execute() throws CommandException {
            start = System.currentTimeMillis();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {  }
            end = System.currentTimeMillis();
            return null;
        }
    }

    private class MyCoordActionReadyXCommand extends CoordActionReadyXCommand {

        private long start;
        private long end;

        public MyCoordActionReadyXCommand(String argJobId) {
            super(argJobId);
        }

        @Override
        protected void loadState() throws CommandException {
        }

        @Override
        protected void executeInterrupts() {
        }

        @Override
        protected void verifyPrecondition() throws CommandException, PreconditionException {
        }

        @Override
        protected Void execute() throws CommandException {
            start = System.currentTimeMillis();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {  }
            end = System.currentTimeMillis();
            return null;
        }
    }

    private class MyCoordActionUpdateXCommand extends CoordActionUpdateXCommand {

        private long start;
        private long end;

        public MyCoordActionUpdateXCommand(WorkflowJobBean workflowJobBean) {
            super(workflowJobBean);
        }

        @Override
        protected void loadState() throws CommandException {
        }

        @Override
        protected void executeInterrupts() {
        }

        @Override
        protected void verifyPrecondition() throws CommandException, PreconditionException {
        }

        @Override
        protected Void execute() throws CommandException {
            start = System.currentTimeMillis();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {  }
            end = System.currentTimeMillis();
            return null;
        }
    }

}