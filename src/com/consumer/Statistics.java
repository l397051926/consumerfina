package com.consumer;

public class Statistics implements Runnable {
    @Override
    public void run() {
        int zongliang=300;

        int onum=0;

        while (true){
            try {
                Thread.sleep(30000);

                int nnum=KafkaMultiProcessorTest.num;

                int dnum=nnum-onum;

                System.out.println("当前消费数量=  "+dnum+"  未消费数量= "+(zongliang-nnum)+" 每秒消费数量= "+(dnum/30)
                        +"  当前消费总大小= "+ Zhuanhuan.getNetFileSizeDescription(dnum*500*1024)+"  每秒消费大小= "+Zhuanhuan.getNetFileSizeDescription(((dnum*500*1024)/30))
                );
                onum=nnum;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
