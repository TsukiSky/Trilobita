package pagerank;

import java.util.concurrent.LinkedBlockingQueue;

import com.trilobita.commons.Mail;
import com.trilobita.engine.server.AbstractServer;

public class Test {

    private class Server {
        LinkedBlockingQueue<Integer> outMailQueue = new LinkedBlockingQueue<>();
    }

    public static void execute(Server server) {
        LinkedBlockingQueue<Integer> outMailQueue = server.outMailQueue;
        LinkedBlockingQueue<Integer> newOutMailQueue = new LinkedBlockingQueue<>();

        while (!outMailQueue.isEmpty()) {
            Integer i = outMailQueue.poll();
            newOutMailQueue.add(i + 100);
            System.out.println(i);
        }
        server.outMailQueue.addAll(newOutMailQueue);
    }

    public static void main(String[] args) throws Exception {
        Test.Server s1 = new Test().new Server();
        s1.outMailQueue.add(1);
        s1.outMailQueue.add(2);
        s1.outMailQueue.add(3);

        System.out.println(s1.outMailQueue);
        execute(s1);
        System.out.println(s1.outMailQueue);
    }
}