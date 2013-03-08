package edu.ucsb.cs.mdcc.txn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;

import edu.ucsb.cs.mdcc.paxos.Transaction;
import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class GradesClient {

    private static boolean verbose = false;
    private static TransactionFactory fac = null;
    
    private static final String LINES = "GRADES_LINE_COUNT";
    private static final String GRADES_LINE = "GRADES_LINE_";
    private static final String STATS_LINE = "STATS_LINE_";

    public static void main(String[] args) throws IOException {
    	
        if (fac == null) {
            fac = new TransactionFactory();
        }
        init();
        System.out.println("Welcome to the MDCC Grades System Client");
        System.out.println("Enter 'help' to see a list of supported commands...\n");

        //printEndpoints();
        System.out.println();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("> ");
            try {
                String command = reader.readLine();
                command = command.trim();
                if ("quit".equals(command)) {
                    break;
                } else if ("read".equals(command)) {
                    read(false);
                } else if ("reads".equals(command)) {
                    read(true);
                } else if ("clean".equals(command)) {
                        Transaction txn = fac.create();
                        try {
                        	txn.begin();
                        	txn.write(LINES, String.valueOf(0).getBytes());
                        	txn.commit();
                        } catch (TransactionException e) {
                        	System.out.println("Failed to clean database");
                        }
                } else if (command.startsWith("appendr")) {
                    if (command.equals("appendr")) {
                        appendRandom(1, 1);
                    } else {
                        String[] commandArgs = command.split(" ");
                        appendRandom(Integer.parseInt(commandArgs[1]), Integer.parseInt(commandArgs[2]));
                    }
                } else if (command.startsWith("append ")) {
                    String[] commandArgs = command.split(" ");
                    if (commandArgs.length != 11) {
                        System.out.println("append must be followed by 10 integers");
                    } else {
                        int[] data = new int[10];
                        for (int i = 0; i < data.length; i++) {
                            data[i] = Integer.parseInt(commandArgs[i + 1]);
                        }
                        if (append(data)) {
                            System.out.println("Append successful");
                        }
                    }
                } /*else if (command.startsWith("sgp")) {
                    String[] commandArgs = command.split(" ");
                    if (commandArgs.length == 2) {
                        setPrimary(gradeServers, commandArgs[1]);
                    } else {
                        printEndpoints();
                    }
                } else if (command.startsWith("ssp")) {
                    String[] commandArgs = command.split(" ");
                    if (commandArgs.length == 2) {
                        setPrimary(statServers, commandArgs[1]);
                    } else {
                        printEndpoints();
                    }
                } */else if ("verbose".equals(command)) {
                    verbose = true;
                    System.out.println("Verbose mode enabled");
                } else if ("silent".equals(command)) {
                    verbose = false;
                    System.out.println("Silent mode enabled");
                } else if ("help".equals(command)) {
                    System.out.println("append [int x 10] - Append the given 10 integers to the database");
                    System.out.println("appendr [concurrency requests] - Append random entries to the database");
                    System.out.println("help - Displays this help message");
                    System.out.println("quit - Quits the Wrench client");
                    System.out.println("read - Read and output the current contents of the database");
                    System.out.println("reads - Read the current contents of the database and report on the consistency level");
                    //System.out.println("sgp [processId] - Set the primary grades server");
                    System.out.println("silent - Turns off the verbose mode");
                    //System.out.println("ssp [processId] - Set the primary stats server");
                    System.out.println("verbose - Activates the verbose mode");
                } else if ("".equals(command)) {
                } else {
                    System.out.println("Unrecognized command: " + command);
                }
            } catch (NumberFormatException e) {
                System.out.println("Number format error: Possible syntax error in command");
            }
        }
        fac.close();
        fac = null;
    }

    private static void init() {
        Transaction txn = fac.create();
        txn.begin();
        try {
            txn.read(LINES);
            txn.commit();
        } catch (TransactionException ignored) {
            try {
                txn.write(LINES, "0".getBytes());
                txn.commit();
            } catch (TransactionException e) {
                System.out.println("Failed to initialize the system");
                System.exit(1);
            }
        }
    }

    private static void read(boolean silent) {
        Transaction txn = fac.create();
        byte[] lineCount = null;
        try {
        	txn.begin();
        	lineCount = txn.read(LINES);
        } catch (TransactionException ignored) {
        }
        
        if (lineCount == null) {
        	System.out.println("No data available");
        } else {
        	int count = Integer.parseInt(Bytes.toString(lineCount));
        	List<String> grades = new ArrayList<String>(count);
        	List<String> stats = new ArrayList<String>(count);
        	
        	for (int i = 0; i < count; i++) {
        		try {
					grades.add(Bytes.toString(txn.read(GRADES_LINE + i)));
					stats.add(Bytes.toString(txn.read(STATS_LINE + i)));
				} catch (TransactionException ignored) {
					log("Failed to read line: " + i, true);
				}
        	}
        	
        	try {
            	txn.commit();
            } catch (TransactionException ignored) {
            }
        	
        	if (grades.size() == stats.size()) {
                boolean valid = true;
                List<String> errors = new ArrayList<String>();
                for (int i = 0; i < grades.size(); i++) {
                    String msg = grades.get(i) + "\t\t" + stats.get(i);
                    if (!silent) {
                        System.out.println(msg);
                    }
                    boolean result = validate(grades.get(i), stats.get(i));
                    if (!result) {
                        errors.add(msg);
                    }
                    valid = valid && result;
                }
                if (valid) {
                    if (grades.size() > 0) {
                        if (!silent) {
                            System.out.println();
                        }
                        System.out.println("Returned " + grades.size() + " lines");
                        System.out.println("All entries consistent...");
                    } else {
                        System.out.println("No data available");
                    }
                } else {
                    log("\nSome inconsistencies detected...", true);
                    for (String error : errors) {
                        log(error, true);
                    }
                }
            } else {
                log("Size Mismatch!", true);
            }
            System.out.flush();
        }
    }

    private static boolean validate(String grades, String stats) {
        String[] gradeValues = grades.split(" ");
        int[] gradeData = new int[gradeValues.length];
        for (int i = 0; i < gradeData.length; i++) {
            gradeData[i] = Integer.parseInt(gradeValues[i]);
        }

        String[] statValues = stats.split(" ");
        double[] statData = new double[statValues.length];
        for (int i = 0; i < statData.length; i++) {
            statData[i] = Double.parseDouble(statValues[i]);
        }

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        double sum = 0D;
        for (int grade : gradeData) {
            if (grade < min) {
                min = grade;
            }
            if (grade > max) {
                max = grade;
            }
            sum += grade;
        }

        return min == (int) statData[0] && max == (int) statData[1] &&
                (sum/gradeData.length) == statData[2];
    }

    private static void appendRandom(int concurrency, int requests) {
        ClientThread[] t = new ClientThread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i] = new ClientThread(requests);
            t[i].start();
        }

        int totalSuccess = 0;
        for (int i = 0; i < concurrency; i++) {
            try {
                t[i].join();
                totalSuccess += t[i].success;
            } catch (InterruptedException ignored) {
            }
        }

        System.out.println(totalSuccess + " of " + (concurrency*requests) + " appends were successful");
    }

    private static boolean append(int[] data) {
    	Transaction txn = fac.create();
    	boolean success = true;
        
        String gradesString = "";
        for (int d : data) {
        	gradesString += d + " ";
        }
        
        
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        double sum = 0D;
        for (int d : data) {
            if (d < min) {
                min = d;
            }
            if (d > max) {
                max = d;
            }
            sum += d;
        }
        String statsString = min + " " + max + " " + sum/data.length;
        
        int lineCount = 0;
        try {
        	txn.begin();
        	lineCount = Integer.parseInt(Bytes.toString(txn.read(LINES)));
        	txn.write(GRADES_LINE + lineCount, gradesString.getBytes());
        	txn.write(STATS_LINE + lineCount, statsString.getBytes());
        	txn.write(LINES, String.valueOf(lineCount + 1).getBytes());
        	txn.commit();
        } catch (TransactionException e) {    	
        	success = false;
        }

        if (success) {
        	log("line " + lineCount + ": " + gradesString + "; " + statsString, false);
        } else {
        	log("Transaction failed on line: " + lineCount, true);
        }
        return success;
    }

    private static void log(String msg, boolean error) {
        if (verbose || error) {
            System.out.println(msg);
        }
    }

    private static class ClientThread extends Thread {

        private int requests;
        private int success = 0;

        private ClientThread(int requests) {
            this.requests = requests;
        }

        @Override
        public void run() {
            Random rand = new Random();

            for (int i = 0; i < requests; i++) {
                int[] data = new int[10];
                for (int j = 0; j < data.length; j++) {
                    data[j] = rand.nextInt(101);
                }
                if (append(data)) {
                    success++;
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {

                }
            }
        }
    }
}
