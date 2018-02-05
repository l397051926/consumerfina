package com.consumer;

public class Maintext {


    public static void main(String[] args) {

        KafkaMultiProcessorTest test = new KafkaMultiProcessorTest();

        //....省略设置topic和group的代码

        test.init();

    }
}
