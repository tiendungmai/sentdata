package com.mycompany.sentdata;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author tiendungmai
 */
public class Test {
    private final Logger logger = Logger.getLogger(Test.class.getName());
    private final static String USER_AGENT = "Sent Data";

    public static void main(String[] args) throws Exception {

        HttpURLConnection connection = null;
        URL url = new URL("http://lg1.logging.admicro.vn/dsp_adx");

        //URL url = new URL("http:localhost");
        /*connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setRequestProperty("Connection", "Keep-Alive");
        connection.setRequestProperty("Content-Language", "en-US");

        connection.setUseCaches(false);
        connection.setDoOutput(true);
        */

        ArrayList<String> lineR = new ArrayList<String>();
        lineR = logPage();

        ArrayList<String> listParam = new ArrayList<String>();
        listParam = listparams(lineR);
        /*Iterator<String> iterator = listParam.iterator();
        while (iterator.hasNext()) {

            String params = iterator.next();
            System.out.println(params);

        }*/

        long start = System.currentTimeMillis();

        System.out.println("Sum reques: " + listParam.size());
        ExecutorService executor = Executors.newFixedThreadPool(10);
        connection = conn(url);
        Iterator<String> iterator = listParam.iterator();
        while (iterator.hasNext()) {

            String params = iterator.next();
            //System.out.println(params);
            GetThread getThread = new GetThread(params,connection);
            executor.submit(getThread);
        }
        connection.disconnect();
        executor.shutdown();

        // Wait until all threads are finish
        while (!executor.isTerminated()) {
        }
        long finish = System.currentTimeMillis();
        long sumtime = (finish - start);
        System.out.println("Time sent: " + (finish - start));
        System.out.println("Request(K)/s: " + listParam.size() / sumtime);

    }

    public static ArrayList<String> listparams(ArrayList<String> line) {

        ArrayList<String> list = new ArrayList<String>();
        Iterator<String> iterator = line.iterator();
        while (iterator.hasNext()) {

            String s = iterator.next();
            String param = getParamsString(s);
            list.add(param);
        }
        return list;
    }

    public static String getParamsString(String s) {
        Map<String, String> urlParameter = new HashMap<String, String>();
        String[] split = s.split("\t");
        int length = split.length;

        urlParameter.put("requestId", split[0]);
        urlParameter.put("sspName", split[1]);
        urlParameter.put("dspName", split[2]);
        urlParameter.put("timestamp", split[3]);
        urlParameter.put("ua", split[4]);
        urlParameter.put("osCode", split[5]);
        urlParameter.put("ip", split[6]);
        urlParameter.put("loc", split[7]);
        urlParameter.put("domain", split[8]);
        urlParameter.put("url", split[9]);
        urlParameter.put("pos", split[10]);
        urlParameter.put("width", split[11]);
        urlParameter.put("height", split[12]);
        urlParameter.put("guid", split[13]);
        urlParameter.put("sspZoneId", split[14]);
        urlParameter.put("dspZoneId", split[15]);
        urlParameter.put("notifyCode", split[16]);
        urlParameter.put("lstBanner", split[17]);
        urlParameter.put("bidPrice", split[18]);
        urlParameter.put("currencyId", split[19]);
        urlParameter.put("browserCode", split[20]);
        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, String> entry : urlParameter.entrySet()) {
            try {
                result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));

                result.append("=");
                result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                result.append("&");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        String resultString = result.toString();
        return resultString.length() > 0
                ? resultString.substring(0, resultString.length() - 1)
                : resultString;
    }

    public static Map<String, String> request(String s) {
        Map<String, String> urlParameter = new HashMap<String, String>();
        String[] split = s.split("\t");
        int length = split.length;

        urlParameter.put("requestId", split[0]);
        urlParameter.put("sspName", split[1]);
        urlParameter.put("dspName", split[2]);
        urlParameter.put("timestamp", split[3]);
        urlParameter.put("ua", split[4]);
        urlParameter.put("osCode", split[5]);
        urlParameter.put("ip", split[6]);
        urlParameter.put("loc", split[7]);
        urlParameter.put("domain", split[8]);
        urlParameter.put("url", split[9]);
        urlParameter.put("pos", split[10]);
        urlParameter.put("width", split[11]);
        urlParameter.put("height", split[12]);
        urlParameter.put("guid", split[13]);
        urlParameter.put("sspZoneId", split[14]);
        urlParameter.put("dspZoneId", split[15]);
        urlParameter.put("notifyCode", split[16]);
        urlParameter.put("lstBanner", split[17]);
        urlParameter.put("bidPrice", split[18]);
        urlParameter.put("currencyId", split[19]);
        urlParameter.put("browserCode", split[20]);
        return urlParameter;
    }


    private static ArrayList<String> logPage() {
        ArrayList<String> logpage = new ArrayList<String>();
        //File folder = new File("/home/tiendungmai/Desktop/sample text");
        File folder = new File("/home/tiendungmai/Desktop/temp");
        File[] listFile = folder.listFiles();
        //System.out.println("so file: " + listFile.length);
        System.out.println("Reading file...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < listFile.length; i++) {
            FileReader fr = null;
            try {
                File f = new File(listFile[i].getPath());
                fr = new FileReader(f);
                BufferedReader br = new BufferedReader(fr);
                String line;
                //ArrayList<String> words = new ArrayList<>();
                while ((line = br.readLine()) != null) {
                    logpage.add(line);
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    fr.close();
                } catch (IOException ex) {
                    Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
        long finish = System.currentTimeMillis();
        System.out.println("Time read file: " + (finish - start));
        //System.out.println(logpage.size());
        System.out.println("Done");


        return logpage;

    }

    static class GetThread implements Runnable {
        private String request;
        private HttpURLConnection connection;
        //private URL url;

        public GetThread(String request,  HttpURLConnection connection ) {
            this.request = request;
            this.connection = connection;
            //this.url = url;
        }


        public void run() {
           // HttpURLConnection connection = null;

            try {

                //Send request
                connection.setRequestProperty("Content-Length",   Integer.toString(request.getBytes().length));
                DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                wr.writeBytes(request);
                wr.flush();
                //System.out.println("done");
                //int responseCode = connection.getResponseCode();

                //print result
              //  System.out.println(responseCode);

                wr.close();
            } catch (MalformedURLException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
                //System.out.println("er");
            }finally {
                //connection.disconnect();
            }

        }
    }

    public static HttpURLConnection conn(URL url ) throws IOException {
        HttpURLConnection conn = null;
        //URL url = new URL("http://lg1.logging.admicro.vn/dsp_adx");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestProperty("USER_AGENT",USER_AGENT);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Content-Language", "en-US");
        return conn;
    }


}
