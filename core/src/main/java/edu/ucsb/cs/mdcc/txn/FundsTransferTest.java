package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.paxos.Transaction;
import edu.ucsb.cs.mdcc.paxos.TransactionException;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FundsTransferTest {

    public static final String ROOT = "ROOT";
    public static final String CHILD = "CHILD";

    public static void main(String[] args) throws TransactionException {
        final TransactionFactory fac = new TransactionFactory();

        final int startingTotal = Integer.parseInt(args[0]);
        final int accounts = Integer.parseInt(args[1]);

        System.out.println("Step 1: Initializing accounts");
        Transaction t1 = fac.create();
        t1.begin();
        t1.write(ROOT, toBytes(startingTotal));
        for (int i = 0; i < accounts; i++) {
            t1.write(CHILD + i, toBytes(0));
        }
        t1.commit();
        System.out.println("Transaction 1 completed...");
        System.out.println("==========================================\n");

        try {
            System.out.println("Starting step 2 in 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        System.out.println("Step 2: Distributing funds");
        Transaction t2 = fac.create();
        t2.begin();
        int root = toInt(t2.read(ROOT));
        System.out.println("ROOT = " + root);
        int[] children = new int[accounts];
        for (int i = 0; i < accounts; i++) {
            int child = toInt(t2.read(CHILD + i));
            children[i] = child;
            System.out.println("CHILD" + i + " = " + child);
        }

        int fundsPerChild = root/accounts;
        for (int i = 0; i < accounts; i++) {
            t2.write(CHILD + i, toBytes(children[i] + fundsPerChild));
            root -= fundsPerChild;
        }
        t2.write(ROOT, toBytes(root));
        t2.commit();
        System.out.println("Transaction 2 completed...");
        System.out.println("==========================================\n");

        try {
            System.out.println("Starting step 3 in 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        System.out.println("Step 3: Gathering funds");
        ExecutorService exec = Executors.newFixedThreadPool(accounts);
        Future[] futures = new Future[accounts];
        for (int i = 0; i < accounts; i++) {
            futures[i] = exec.submit(new FundTransferTask(fac, i));
        }

        for (int i = 0; i < accounts; i++) {
            try {
                futures[i].get();
            } catch (InterruptedException ignored) {
            } catch (ExecutionException ignored) {
            }
        }
        exec.shutdownNow();
        System.out.println("==========================================\n");

        try {
            System.out.println("Starting step 4 in 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        System.out.println("Step 4: Validating funds");
        Transaction t4 = fac.create();
        t4.begin();
        int total = 0;
        root = toInt(t4.read(ROOT));
        System.out.println("ROOT = " + root);
        children = new int[accounts];
        total += root;
        for (int i = 0; i < accounts; i++) {
            int child = toInt(t4.read(CHILD + i));
            children[i] = child;
            total += child;
            System.out.println(CHILD + i + " = " + child);
        }

        t4.commit();
        System.out.println("Transaction 4 completed...");
        System.out.println("==========================================\n");
        System.out.println("FINAL TOTAL = " + total);
    }

    private static byte[] toBytes(int value) {
        String str = String.valueOf(value);
        return str.getBytes();
    }

    private static int toInt(byte[] value) {
        String str = new String(value);
        return Integer.parseInt(str);
    }

    private static class FundTransferTask implements Runnable {

        private TransactionFactory fac;
        private int index;

        private FundTransferTask(TransactionFactory fac, int index) {
            this.fac = fac;
            this.index = index;
        }

        public void run() {
            try {
                try {
                    Thread.sleep(new Random().nextInt(10) * 500);
                } catch (InterruptedException ignored) {
                }
                Transaction t3 = fac.create();
                t3.begin();
                int root = toInt(t3.read(ROOT));
                System.out.println("ROOT = " + root);
                int child = toInt(t3.read(CHILD + index));
                System.out.println(CHILD + index + " = " + child);

                t3.write(ROOT, toBytes(root + child));
                t3.write(CHILD + index, toBytes(0));
                t3.commit();
                System.out.println("Transaction 3 completed by Thread-" + index);
            } catch (TransactionException e) {
                System.out.println("Transaction 3 failed by Thread-" + index);
            }
        }
    }

}
