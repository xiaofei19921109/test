package com.thoughtworks;

import org.junit.Assert;
import org.junit.Test;

public class GraPGTest {

    @Test
    public void print() {
        GraPG g = new GraPG();
        String res = g.print();
        Assert.assertEquals("hello world",res);
    }
}