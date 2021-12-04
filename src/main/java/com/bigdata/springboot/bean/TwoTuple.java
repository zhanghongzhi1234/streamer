package com.bigdata.springboot.bean;

public class TwoTuple<A, B> {
    private final A first;
    private final B second;

    public TwoTuple(A a, B b) {
        this.first = a;
        this.second = b;
    }

    public A getFirst() {
        return this.first;
    }

    public B getSecond() {
        return this.second;
    }
}

