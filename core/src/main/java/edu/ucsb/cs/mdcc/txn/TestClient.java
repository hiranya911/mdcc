package edu.ucsb.cs.mdcc.txn;

import java.nio.ByteBuffer;

import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class TestClient {

    public static void main(String[] args) {
        /*LocalTransaction txn1 = new LocalTransaction();
        try {
            txn1.begin();
            txn1.write("foo", ByteBuffer.wrap("2".getBytes()));
            txn1.write("bar", ByteBuffer.wrap("2".getBytes()));
            txn1.commit();
            System.out.println("Txn 1 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }*/

        /*LocalTransaction txn2 = new LocalTransaction();
        try {
            txn2.begin();
            ByteBuffer object1 = txn2.read("foo");
            ByteBuffer object2 = txn2.read("bar");
            System.out.println("Foo = " + new String(object1.array()));
            System.out.println("Bar = " + new String(object2.array()));
            txn2.write("bar", ByteBuffer.wrap("Bar Value 2".getBytes()));
            txn2.commit();
            System.out.println("Txn 2 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }*/

        LocalTransaction txn3 = new LocalTransaction();
        try {
            txn3.begin();
            ByteBuffer object1 = txn3.read("foo");
            ByteBuffer object2 = txn3.read("bar");
            System.out.println("Foo = " + new String(object1.array()));
            System.out.println("Bar = " + new String(object2.array()));
            txn3.commit();
            System.out.println("Txn 3 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
        }
    }
}
