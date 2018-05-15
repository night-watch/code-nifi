package nifi.bicon.test;

import org.junit.Test;

/**
 * Created by NightWatch on 2018/2/5.
 */
public class GC {


    public static void main(String[] args) {

        System.out.println(args[0]);
        System.out.println(args[1]);
        System.out.println(args[2]);
        System.out.println(args.length);
        B b = new B(true);

        b.ch();

        new B(true);

        new A();

        System.out.printf("");

//        System.gc();


    }

    @Test
    public void test(){

        GC.main(new String[]{"0","1","2","3"});

    }





}

class B {

    boolean x = false;

    public B(boolean x) {
        System.out.println(x);

        this.x = x;
    }

    public void ch() {
        x = !x;
    }

    public void finalize() {
        System.out.println("finalize");
    }

}

class A {
    static {
        System.out.println("A");
    }

    A() {
        System.out.println("AB");
    }

}
