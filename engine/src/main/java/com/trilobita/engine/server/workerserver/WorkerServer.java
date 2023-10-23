package com.trilobita.engine.server.workerserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.computing.task.MailingTask;
import com.trilobita.engine.computing.task.Task;
import com.trilobita.engine.computing.task.VertexTask;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.common.ServerStatus;
import com.trilobita.engine.util.Hardware;
import com.trilobita.exception.TrilobitaException;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.*;

/**
 * Worker Server controls vertex computations and communicate with other servers
 */
@Slf4j
public class WorkerServer extends AbstractServer {
    private ExecutorService executorService;
    private LinkedBlockingQueue<Task> vertexTasks;  // vertex-related tasks queue
    private ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    private ScheduledExecutorService inMailService;
    private MessageConsumer partitionMessageConsumer;
    private MessageConsumer startMessageConsumer;
    private CountDownLatch latch;


    public WorkerServer(int serverId) throws ExecutionException, InterruptedException {
        super(serverId);
        initialize();
    }

    public WorkerServer(int serverId, int numOfExecutor) throws ExecutionException, InterruptedException {
        super(serverId);
        initialize();
        this.executorService = Executors.newFixedThreadPool(numOfExecutor);
    }

    @Override
    public void initialize() throws ExecutionException, InterruptedException {
        this.executorService = Executors.newFixedThreadPool(Hardware.getCoreNum()); // number of thread is the number of cores on current machine by default
        this.inMailService = Executors.newScheduledThreadPool(1);
        this.vertexTasks = new LinkedBlockingQueue<>();
        this.outMailTable = new ConcurrentHashMap<>();
        this.setServerStatus(ServerStatus.START);
        this.partitionMessageConsumer= new MessageConsumer(this.getServerId() + "partition", new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException {
                ObjectMapper objectMapper = new ObjectMapper();
                vertexGroup = objectMapper.convertValue(value.getMessage().getContent(), VertexGroup.class);
                log.info("Vertex Group: "+vertexGroup);
                start();
            }
        });
        this.startMessageConsumer = new MessageConsumer("start", new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException {
                log.info("start new super step...");
                execute();
            }
        });
        startMessageConsumer.start();
        partitionMessageConsumer.start();
    }

    @Override
    public void start() throws InterruptedException, ExecutionException {
        execute();
    }

    private void execute() throws InterruptedException {
        log.info("entering new super step...");
        // Superstep 1.1 Handling inMailQueue
        // TODO: improve the inMail handling mechanism -> current solution has flaws
        latch = new CountDownLatch(1);
        this.inMailService.scheduleAtFixedRate(()->{
            while(!this.getInMailQueue().isEmpty()) {
                Mail mail = this.getInMailQueue().poll();
                this.distributeMailToVertex(mail);
            }
            latch.countDown();
        }, 0, 1, TimeUnit.SECONDS);
        latch.await();
        log.info("finish distributing task");

        // Tell the vertices to start superstep
        latch = new CountDownLatch(vertexGroup.getVertexSet().size());
        for (Vertex v: vertexGroup.getVertexSet()){
            Runnable wrappedTask = () -> {
                try {
                    v.startSuperstep();
                    BlockingQueue<Mail> mails = v.getIncomingQueue();
                    // Iterate through vertices and add tasks to the task queue
                    for (Mail mail: mails){
                        Task task = new VertexTask(v, mail.getMessage());
                        vertexTasks.add(task);
                    }
                }
                finally {
                    latch.countDown();
                }
            };
            executorService.execute(wrappedTask);
        }
        latch.await();
        log.info("finish adding tasks and starting superstep");

        // Superstep 1.3 Handling vertex tasks
        latch = new CountDownLatch(vertexTasks.size());
        while (!vertexTasks.isEmpty()) {
            Runnable task = vertexTasks.poll();
            Runnable wrappedTask = () -> {
                try {
                    task.run();
                }
                finally {
                    latch.countDown();
                }
            };
            executorService.execute(wrappedTask);
        }
        latch.await();
        log.info("finish executing vertex tasks");

        // Superstep 1.4 Handling outMailQueue
        latch = new CountDownLatch(super.getOutMailQueue().size());
        while (!super.getOutMailQueue().isEmpty()) {
            Mail mail = this.getOutMailQueue().poll();
            int receiverId = findServerByVertexId(mail.getToVertexId());
            Runnable mailingTask = new MailingTask(this.getOutMailQueue().poll(), receiverId);
            Runnable wrappedTask = () -> {
                try {
                    mailingTask.run();
                }
                finally {
                    latch.countDown();
                }
            };

            executorService.execute(wrappedTask);
        }
        latch.await();
        log.info("finish sending out mails");

        // Tell the master it has finished its job
        MessageProducer.produce(null, new Mail(-1,null,MailType.FINISH_INDICATOR), "finish");
    }

    @Override
    public void pause() {

    }

    @Override
    public void shutdown() {
        this.executorService.shutdown();
        this.inMailService.shutdown();
    }

    public void onStartSignal() throws InterruptedException, ExecutionException {
        this.start();
    }

    public void sendCompleteSignal() {

    }

    public void distributeMailToVertex(Mail mail) {
        Vertex vertex = findVertexById(mail.getToVertexId());
        // TODO: send mail to the corresponding vertex
        assert vertex != null;
        vertex.onReceive(mail);
    }

    private Vertex findVertexById(int vertexId) {
        try {
            return this.getVertexGroup().getVertexById(vertexId);
        } catch (TrilobitaException e) {
            e.printStackTrace();
        }
        return null;
    }
}
