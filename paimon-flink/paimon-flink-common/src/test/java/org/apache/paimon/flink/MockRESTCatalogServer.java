package org.apache.paimon.flink;

import java.io.IOException;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

/** Mock REST server for testing. */
public class MockRESTCatalogServer {
    public static void main(String[] args) {
        MockWebServer server = new MockWebServer();
        final Dispatcher dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                switch (request.getPath()) {
                    case "/v1/login/auth/":
                        return new MockResponse().setResponseCode(200);
                    case "/v1/check/version/":
                        return new MockResponse().setResponseCode(200).setBody("version=9");
                    case "/v1/profile/info":
                        return new MockResponse().setResponseCode(200).setBody("profile");
                }
                return new MockResponse().setResponseCode(404);
            }
        };
        server.setDispatcher(dispatcher);
        try {
            server.start(8099);
            String serverUrl = server.url("").toString();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        while (true) {
            try {
                Thread.sleep(1000);
                RecordedRequest request = server.takeRequest();
                System.out.println("Request: " + request.getPath());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
