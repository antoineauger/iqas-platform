package fr.isae.iqas.server;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.apache.http.HttpHeaders.USER_AGENT;

/**
 * Created by an.auger on 30/03/2017.
 */
public class HttpUtils {

    public static boolean checkIfEndpointIsAvailable(String url) {
        boolean success = false;
        HttpClient client = HttpClientBuilder.create().disableAutomaticRetries().build();
        HttpGet request = new HttpGet(url);
        request.addHeader("User-Agent", USER_AGENT);
        HttpResponse response;
        try {
            response = client.execute(request);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                success = true;
            }
        } catch (Exception e) {
        }
        return success;
    }

    // TODO modify according Virtual Sensor API
    public static String sendGet(String url) throws Exception {
        HttpClient client = HttpClientBuilder.create().disableAutomaticRetries().build();
        HttpGet request = new HttpGet(url);
        request.addHeader("User-Agent", USER_AGENT);

        HttpResponse response = client.execute(request);

        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " +
                response.getStatusLine().getStatusCode());

        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuilder result = new StringBuilder();
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        return result.toString();
    }

    // TODO modify according Virtual Sensor API
    public static String sendPost(String url) throws Exception {
        HttpClient client = HttpClientBuilder.create().disableAutomaticRetries().build();
        HttpPost post = new HttpPost(url);
        post.setHeader("User-Agent", USER_AGENT);

        List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
        urlParameters.add(new BasicNameValuePair("sn", "C02G8416DRJM"));
        urlParameters.add(new BasicNameValuePair("cn", ""));
        urlParameters.add(new BasicNameValuePair("locale", ""));
        urlParameters.add(new BasicNameValuePair("caller", ""));
        urlParameters.add(new BasicNameValuePair("num", "12345"));

        post.setEntity(new UrlEncodedFormEntity(urlParameters));

        HttpResponse response = client.execute(post);
        System.out.println("\nSending 'POST' request to URL : " + url);
        System.out.println("Post parameters : " + post.getEntity());
        System.out.println("Response Code : " +
                response.getStatusLine().getStatusCode());

        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuilder result = new StringBuilder();
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        return result.toString();
    }

}
