package com.trilobita.engine.server.workerserver;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.computing.task.MailingTask;
import com.trilobita.engine.computing.task.Task;
import com.trilobita.engine.computing.task.VertexTask;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.common.ServerStatus;
import com.trilobita.engine.util.Hardware;
import com.trilobita.exception.TrilobitaException;

import java.util.concurrent.*;

/**
 * Worker Server controls vertex computations and communicate with other servers
 */
public class WorkerServer extends AbstractServer {
    private ExecutorService executorService;
    private LinkedBlockingQueue<VertexTask> vertexTasks;  // vertex-related tasks queue
    private ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    private ScheduledExecutorService inMailService;

    public WorkerServer(int serverId) {
        super(serverId);
        initialize();
    }

    public WorkerServer(int serverId, int numOfExecutor) {
        this(serverId);
        this.executorService = Executors.newFixedThreadPool(numOfExecutor);
    }

    @Override
    public void initialize() {
        this.executorService = Executors.newFixedThreadPool(Hardware.getCoreNum()); // number of thread is the number of cores on current machine by default
        this.inMailService = Executors.newScheduledThreadPool(1);
        this.vertexTasks = new LinkedBlockingQueue<>();
        this.outMailTable = new ConcurrentHashMap<>();
        this.setServerStatus(ServerStatus.START);
    }

    @Override
    public void start() {
        // Superstep 1.1 Handling vertex tasks
        while (!vertexTasks.isEmpty()) {
            executorService.execute(vertexTasks.poll());
        }

        // Superstep 1.2 Handling outMailQueue
        while (!super.getOutMailQueue().isEmpty()) {
            Mail mail = this.getOutMailQueue().poll();
            int receiverId = findServerByVertexId(mail.getToVertexId());
            executorService.execute(new MailingTask(this.getOutMailQueue().poll(), receiverId));
        }

        try {
            if (executorService.awaitTermination(100, TimeUnit.SECONDS)) {
                // All tasks are completed
            } else {

            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Superstep 1.3 Handling inMailQueue
        // TODO: improve the inMail handling mechanism -> current solution has flaws
        this.inMailService.scheduleAtFixedRate(()->{
            while(!this.getInMailQueue().isEmpty()) {
                Mail mail = this.getInMailQueue().poll();
                this.distributeMailToVertex(mail);
            }
        }, 0, 1, TimeUnit.SECONDS);
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

    public void distributeMailToVertex(Mail mail) {
        Vertex vertex = findVertexById(mail.getToVertexId());
        if (vertex != null) {
            vertex.onReceive(mail);
        }
    }

    private Vertex findVertexById(int vertexId) {
        return this.getVertexGroup().getVertexById(vertexId);
    }
}
