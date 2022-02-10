package com.aliyun.adb.contest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class VmstatLogger implements Runnable {

    public static void useLinuxCommond(){
        try {
            Process p = Runtime.getRuntime().exec("lscpu");  //调用Linux的相关命令-列出cpu信息
//            Process p = Runtime.getRuntime().exec("gnome-terminal -e 'bash -c ls; exec bash'");  //调用Linux的相关命令-列出cpu信息
//            Process p = Runtime.getRuntime().exec("s-tui");  //调用Linux的相关命令
//            Process p = Runtime.getRuntime().exec("vmstat");  //调用Linux的相关命令
//            Process p = Runtime.getRuntime().exec("vmstat -n 3");  //调用Linux的相关命令
//            Process p = Runtime.getRuntime().exec("cpupower frequency-info");  //调用Linux的相关命令
            InputStreamReader ir = new InputStreamReader(p.getInputStream());
            LineNumberReader input = new LineNumberReader (ir);      //创建IO管道，准备输出命令执行后的显示内容
            String line;
            while ((line = input.readLine ()) != null){     //按行打印输出内容
                System.out.println(line+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void useIOStat(){
        try {
//            Process p = Runtime.getRuntime().exec("lscpu");  //调用Linux的相关命令-列出cpu信息
//            Process p = Runtime.getRuntime().exec("s-tui");  //调用Linux的相关命令
//            Process p = Runtime.getRuntime().exec("vmstat");  //调用Linux的相关命令
            /**
             * root@ubuntu:~# vmstat 2 1
             * procs -----------memory---------- ---swap-- -----io---- -system-- ----cpu----
             *  r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa
             *  1  0      0 3498472 315836 3819540    0    0     0     1    2    0  0  0 100  0
             * 2表示每个两秒采集一次服务器状态，1表示只采集一次。
             */

            Process p = Runtime.getRuntime().exec("iostat 2 1000");  //调用Linux的相关命令
            InputStreamReader ir = new InputStreamReader(p.getInputStream());
            LineNumberReader input = new LineNumberReader(ir);      //创建IO管道，准备输出命令执行后的显示内容

            String line;
            while ((line = input.readLine ()) != null){     //按行打印输出内容
                System.out.println(line+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void useLinuxCommond2(){
        try {
//            Process p = Runtime.getRuntime().exec("lscpu");  //调用Linux的相关命令-列出cpu信息
//            Process p = Runtime.getRuntime().exec("s-tui");  //调用Linux的相关命令
//            Process p = Runtime.getRuntime().exec("vmstat");  //调用Linux的相关命令
            /**
             * root@ubuntu:~# vmstat 2 1
             * procs -----------memory---------- ---swap-- -----io---- -system-- ----cpu----
             *  r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa
             *  1  0      0 3498472 315836 3819540    0    0     0     1    2    0  0  0 100  0
             * 2表示每个两秒采集一次服务器状态，1表示只采集一次。
             */

            Process p = Runtime.getRuntime().exec("vmstat 2 1000");  //调用Linux的相关命令
            InputStreamReader ir = new InputStreamReader(p.getInputStream());
            LineNumberReader input = new LineNumberReader(ir);      //创建IO管道，准备输出命令执行后的显示内容

            String line;
            while ((line = input.readLine ()) != null){     //按行打印输出内容
                System.out.println(line+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        useLinuxCommond();
        useLinuxCommond2();
    }
}
