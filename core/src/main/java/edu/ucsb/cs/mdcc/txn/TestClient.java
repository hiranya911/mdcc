package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class TestClient {

    public static void main(String[] args) {
        LocalTransaction txn1 = new LocalTransaction();
        try {
            txn1.begin();
            txn1.write("foo", "Foo Value");
            txn1.write("bar", "Bar Value");
            txn1.commit();
            System.out.println("Txn 1 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        LocalTransaction txn2 = new LocalTransaction();
        try {
            txn2.begin();
            Object object1 = txn2.read("foo");
            Object object2 = txn2.read("bar");
            System.out.println("Foo = " + object1);
            System.out.println("Bar = " + object2);
            txn2.write("bar", "Bar Value 2");
            txn2.commit();
            System.out.println("Txn 2 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        LocalTransaction txn3 = new LocalTransaction();
        try {
            txn3.begin();
            Object object1 = txn3.read("foo");
            Object object2 = txn3.read("bar");
            System.out.println("Foo = " + object1);
            System.out.println("Bar = " + object2);
            txn3.commit();
            System.out.println("Txn 3 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
        }
    }
}
