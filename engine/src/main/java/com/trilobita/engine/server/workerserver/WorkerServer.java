package com.trilobita.engine.server.workerserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.engine.computing.task.MailingTask;
import com.trilobita.engine.computing.task.Task;
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
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException {
                ObjectMapper objectMapper = new ObjectMapper();
                vertexGroup = objectMapper.convertValue(value.getMessage().getContent(), VertexGroup.class);
                log.info("Vertex Group: "+vertexGroup);
                start();
            }
        });
        listenForPartition();
    }

    @Override
    public void start() {

        // Superstep 1.1 Handling inMailQueue
        // TODO: improve the inMail handling mechanism -> current solution has flaws
        this.inMailService.scheduleAtFixedRate(()->{
            while(!this.getInMailQueue().isEmpty()) {
                Mail mail = this.getInMailQueue().poll();
                this.distributeMailToVertex(mail);
            }
        }, 0, 1, TimeUnit.SECONDS);

        for (Vertex v: vertexGroup.getVertexSet()){
            v.startSuperstep();
        }

        // iterate through vertices and add tasks to the task queue

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
        try {
            latch.await();
        } catch (InterruptedException e) {
            // Handle the exception
            Thread.currentThread().interrupt();
        }


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
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pause() {

    }

    @Override
    public void shutdown() {
        this.executorService.shutdown();
        this.inMailService.shutdown();
    }

    public void onStartSignal() {
        this.start();
    }

    public void sendCompleteSignal() {

    }

    public void listenForPartition() throws ExecutionException, InterruptedException {
        partitionMessageConsumer.start();
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
