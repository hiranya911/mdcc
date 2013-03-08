package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.MDCCException;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.paxos.Transaction;
import edu.ucsb.cs.mdcc.paxos.TransactionException;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class MDCCClient {

    private static final ExecutorService exec = Executors.newCachedThreadPool();

    private static final AtomicBoolean silent = new AtomicBoolean(false);

    public static void main(String[] args) throws IOException {
        TransactionFactory fac = new TransactionFactory();

        System.out.println("Welcome to MDCC Client");
        System.out.println("Enter 'help' to see a list of supported commands...");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        Options options = new Options();
        options.addOption("k", true, "Key of the data object");
        options.addOption("v", true, "Value of the data object");
        options.addOption("c", true, "Number of concurrent users to emulate");
        options.addOption("n", true, "Number of transactions executed by each user");
        options.addOption("t", true, "Total number of unique keys");
        options.addOption("w", true, "Number of unique keys per worker");
        options.addOption("silent", false, "Enable silent mode");

        CommandLineParser parser = new BasicParser();
        MDCCConfiguration config = MDCCConfiguration.getConfiguration();

        while (true) {
            CommandLine cmd;
            String key = "DefaultKey";
            String value = "DefaultValue";
            int concurrency = 1;
            int num = 1;
            int total = 1;
            int keysPerWorker = 1;

            System.out.print("> ");
            String command = reader.readLine();
            String[] cmdArgs = translateCommandline(command);
            try {
                cmd = parser.parse(options, cmdArgs);
            } catch (ParseException e) {
                System.out.println("Invalid command: " + command);
                continue;
            }

            if (cmdArgs.length == 0) {

            } else if ("get".equals(cmdArgs[0])) {
                if (cmd.hasOption("k")) {
                    key = cmd.getOptionValue("k");
                }
                readOnlyTransaction(fac, key);
            } else if ("put".equals(cmdArgs[0])) {
                if (cmd.hasOption("k")) {
                    key = cmd.getOptionValue("k");
                }
                if (cmd.hasOption("v")) {
                    value = cmd.getOptionValue("v");
                }
                blindWriteTransaction(fac, key, value);
            } else if ("getr".equals(cmdArgs[0])) {
                if (cmd.hasOption("c")) {
                    concurrency = Integer.parseInt(cmd.getOptionValue("c"));
                }
                if (cmd.hasOption("n")) {
                    num = Integer.parseInt(cmd.getOptionValue("n"));
                }
                if (cmd.hasOption("t")) {
                    total = Integer.parseInt(cmd.getOptionValue("t"));
                }
                if (cmd.hasOption("w")) {
                    keysPerWorker = Integer.parseInt(cmd.getOptionValue("w"));
                }
                if (cmd.hasOption("silent")) {
                    silent.compareAndSet(false, true);
                }
                randomReadOnlyTransactions(fac, concurrency, num, keysPerWorker, total);
                silent.compareAndSet(true, false);
            } else if ("putr".equals(cmdArgs[0])) {
                if (cmd.hasOption("c")) {
                    concurrency = Integer.parseInt(cmd.getOptionValue("c"));
                }
                if (cmd.hasOption("n")) {
                    num = Integer.parseInt(cmd.getOptionValue("n"));
                }
                if (cmd.hasOption("t")) {
                    total = Integer.parseInt(cmd.getOptionValue("t"));
                }
                if (cmd.hasOption("w")) {
                    keysPerWorker = Integer.parseInt(cmd.getOptionValue("w"));
                }
                if (cmd.hasOption("silent")) {
                    silent.compareAndSet(false, true);
                }
                randomBlindWriteTransactions(fac, concurrency, num, keysPerWorker, total);
                silent.compareAndSet(true, false);
            } else if ("primary".equals(cmdArgs[0])) {
                if (cmdArgs.length == 2) {
                    if (!config.reorderMembers(cmdArgs[1])) {
                        System.out.println("Invalid server ID: " + cmdArgs[1]);
                    }
                }

                if (fac.isLocal()) {
                    Member primary = config.getMembers()[0];
                    System.out.print("Primary Storage Node: " + primary.getProcessId() + " [");
                    System.out.println(primary.getHostName() + ":" + primary.getPort() + "]");
                } else {
                    System.out.print("Primary App Server: " + config.getAppServerUrl());
                }
            } else if ("servers".equals(cmdArgs[0])) {
                if (fac.isLocal()) {
                    boolean first = true;
                    for (Member member : config.getMembers()) {
                        System.out.print(member.getProcessId() + " [");
                        System.out.print(member.getHostName() + ":" + member.getPort() + "]");
                        if (first) {
                            System.out.print(" [Primary]");
                            first = false;
                        }
                        System.out.println();
                    }
                } else {
                    System.out.print("App Server: " + config.getAppServerUrl());
                }
            } else if ("quit".equals(cmdArgs[0])) {
                break;
            } else if ("help".equals(cmdArgs[0])) {
                System.out.println("get -k key             Retrieve the object with the specified key\n");
                System.out.println("put -k key -v value    Write the given value to the object with the specified key\n");
                System.out.println("getr -c concur -n requests -t totalKeys -w keysPerWorker");
                System.out.println("                       Execute random read operations using multiple concurrent workers\n");
                System.out.println("putr -c concur -n requests -t totalKeys -w keysPerWorker");
                System.out.println("                       Execute random write operations using multiple concurrent workers\n");
                System.out.println("primary [serverId]     Display/Set the primary backend server\n");
                System.out.println("servers                List all the backend servers\n");
                System.out.println("help                   Display this help message\n");
                System.out.println("quit                   Terminate the client application\n");
            } else {
                System.out.println("Unrecognized command: " + command);
            }
        }
        exec.shutdownNow();
        fac.close();
    }

    private static void readOnlyTransaction(TransactionFactory fac, String key) {
        Transaction txn = fac.create();
        try {
            txn.begin();
            byte[] value = txn.read(key);
            System.out.println(key + ": " + new String(value));
            txn.commit();
        } catch (TransactionException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void randomReadOnlyTransactions(final TransactionFactory fac, int c, int r, int k, int t) {
        List<Future> futures = new ArrayList<Future>();

        final List<String> keys = new ArrayList<String>();
        for (int i = 0; i < t; i++) {
            keys.add("KEY" + i);
        }

        int success = 0;
        int failure = 0;
        IndexedReadWorker[] workers = new IndexedReadWorker[c];
        for (int i = 0; i < c; i++) {
            workers[i] = new IndexedReadWorker(i, fac, r, k, keys);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < c; i++) {
            futures.add(exec.submit(workers[i]));
        }

        for (int i = 0; i < c; i++) {
            try {
                futures.get(i).get();
            } catch (Exception ignored) {
            }
            success += workers[i].success;
            failure += workers[i].failure;
        }
        long end = System.currentTimeMillis();
        int total = success + failure;
        System.out.println("\nSuccessful: " + success + "/" + total);
        System.out.println("Failed: " + failure + "/" + total);
        System.out.println("Time elapsed: " + (end - start) + "ms");
    }

    private static void randomBlindWriteTransactions(final TransactionFactory fac, int c, int r, int k, int t) {
        List<Future> futures = new ArrayList<Future>();

        final List<String> keys = new ArrayList<String>();
        for (int i = 0; i < t; i++) {
            keys.add("KEY" + i);
        }

        int success = 0;
        int failure = 0;
        IndexedWriteWorker[] workers = new IndexedWriteWorker[c];
        for (int i = 0; i < c; i++) {
            workers[i] = new IndexedWriteWorker(i, fac, r, k, keys);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < c; i++) {
            futures.add(exec.submit(workers[i]));
        }

        for (int i = 0; i < c; i++) {
            try {
                futures.get(i).get();
            } catch (Exception ignored) {
            }
            success += workers[i].success;
            failure += workers[i].failure;
        }
        long end = System.currentTimeMillis();

        int total = success + failure;
        System.out.println("\nSuccessful: " + success + "/" + total);
        System.out.println("Failed: " + failure + "/" + total);
        System.out.println("Time elapsed: " + (end - start) + "ms");
    }

    private static void blindWriteTransaction(TransactionFactory fac, String key, String value) {
        Transaction txn = fac.create();
        try {
            txn.begin();
            txn.write(key, value.getBytes());
            System.out.println(key + ": " + value);
            txn.commit();
        } catch (TransactionException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static class IndexedReadWorker implements Runnable {

        int index;
        TransactionFactory fac;
        int requests;
        int keyCount;
        List<String> keys;
        int success;
        int failure;

        private IndexedReadWorker(int index, TransactionFactory fac, int requests,
                                  int keyCount, List<String> keys) {
            this.index = index;
            this.fac = fac;
            this.requests = requests;
            this.keyCount = keyCount;
            this.keys = keys;
            this.success = 0;
            this.failure = 0;
        }

        public void run() {
            int baseKey = (index * keyCount) % keys.size();
            for (int i = 0; i < requests; i++) {
                int keyIndex = (baseKey + (i % keyCount)) % keys.size();
                String key = keys.get(keyIndex);
                Transaction txn = fac.create();
                try {
                    txn.begin();
                    byte[] value = txn.read(key);
                    txn.commit();
                    if (!silent.get()) {
                        System.out.println("[Thread-" + index + "] " + key + ": " + new String(value));
                    }
                    success++;
                } catch (Exception e) {
                    if (!silent.get()) {
                        System.out.println("[Thread-" + index + "] Error: " + e.getMessage());
                    }
                    failure++;
                }
            }
        }
    }

    private static class IndexedWriteWorker implements Runnable {

        int index;
        TransactionFactory fac;
        int requests;
        int keyCount;
        List<String> keys;
        int success;
        int failure;

        private IndexedWriteWorker(int index, TransactionFactory fac, int requests,
                                   int keyCount, List<String> keys) {
            this.index = index;
            this.fac = fac;
            this.requests = requests;
            this.keyCount = keyCount;
            this.keys = keys;
            this.success = 0;
            this.failure = 0;
        }

        public void run() {
            int baseKey = (index * keyCount) % keys.size();
            for (int i = 0; i < requests; i++) {
                int keyIndex = (baseKey + (i % keyCount)) % keys.size();
                String key = keys.get(keyIndex);
                Transaction txn = fac.create();
                try {
                    txn.begin();
                    String value = "random_value_" + index + "_" + i + "_" + System.currentTimeMillis();
                    txn.write(key, value.getBytes());
                    txn.commit();
                    if (!silent.get()) {
                        System.out.println("[Thread-" + index + "] " + key + ": " + value);
                    }
                    success++;
                } catch (TransactionException e) {
                    if (!silent.get()) {
                        System.out.println("[Thread-" + index + "] Error: " + e.getMessage());
                    }
                    failure++;
                }
            }
        }
    }

    /*
     * Code lifted from Apache ANT with thanks
     */
    private static String[] translateCommandline(String toProcess) {
        if (toProcess == null || toProcess.length() == 0) {
            //no command? no string
            return new String[0];
        }
        // parse with a simple finite state machine

        final int normal = 0;
        final int inQuote = 1;
        final int inDoubleQuote = 2;
        int state = normal;
        StringTokenizer tok = new StringTokenizer(toProcess, "\"\' ", true);
        Vector<String> v = new Vector<String>();
        StringBuffer current = new StringBuffer();
        boolean lastTokenHasBeenQuoted = false;

        while (tok.hasMoreTokens()) {
            String nextTok = tok.nextToken();
            switch (state) {
                case inQuote:
                    if ("\'".equals(nextTok)) {
                        lastTokenHasBeenQuoted = true;
                        state = normal;
                    } else {
                        current.append(nextTok);
                    }
                    break;
                case inDoubleQuote:
                    if ("\"".equals(nextTok)) {
                        lastTokenHasBeenQuoted = true;
                        state = normal;
                    } else {
                        current.append(nextTok);
                    }
                    break;
                default:
                    if ("\'".equals(nextTok)) {
                        state = inQuote;
                    } else if ("\"".equals(nextTok)) {
                        state = inDoubleQuote;
                    } else if (" ".equals(nextTok)) {
                        if (lastTokenHasBeenQuoted || current.length() != 0) {
                            v.addElement(current.toString());
                            current = new StringBuffer();
                        }
                    } else {
                        current.append(nextTok);
                    }
                    lastTokenHasBeenQuoted = false;
                    break;
            }
        }
        if (lastTokenHasBeenQuoted || current.length() != 0) {
            v.addElement(current.toString());
        }
        if (state == inQuote || state == inDoubleQuote) {
            throw new MDCCException("unbalanced quotes in " + toProcess);
        }
        String[] args = new String[v.size()];
        v.copyInto(args);
        return args;
    }

}
