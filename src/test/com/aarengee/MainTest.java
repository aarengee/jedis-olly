package com.aarengee;

import org.junit.jupiter.api.Test;

import java.util.Objects;

public class MainTest {

  @Test
  public void testMain1(){
    assert Objects.equals(Main.main1(new String[]{"s"})[0], "s");
  }

}