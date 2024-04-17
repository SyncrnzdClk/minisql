import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import entities.Card;
import queries.ApiResult;
import queries.CardList;
import utils.ConnectConfig;
import utils.DatabaseConnector;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.logging.Logger;



public class Main {
    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static LibraryManagementSystem library;

    public static void main(String[] args) throws IOException {
        try {
            // parse connection config from "resources/application.yaml"
            ConnectConfig conf = new ConnectConfig();
            log.info("Success to parse connect config. " + conf.toString());
            // connect to database
            DatabaseConnector connector = new DatabaseConnector(conf);
            boolean connStatus = connector.connect();
            if (!connStatus) {
                log.severe("Failed to connect database.");
                System.exit(1);
            }
            /* do somethings */
            library = new LibraryManagementSystemImpl(connector);
            // release database connection handler
            if (connector.release()) {
                log.info("Success to release connection.");
            } else {
                log.warning("Failed to release connection.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 创建HTTP服务器，监听指定端口
        // 这里是8000，建议不要80端口，容易和其他的撞
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);

        // 添加handler，这里就绑定到/card路由
        // 所以localhost:8000/card是会有handler来处理
        server.createContext("/card", new CardHandler());

        // 启动服务器
        server.start();

        // 标识一下，这样才知道我的后端启动了（确信
        System.out.println("Server is listening on port 8000");
    }

    // 剩下的Main class...
    static class CardHandler implements HttpHandler {
        // 关键重写handle方法
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // 允许所有域的请求，cors处理
            Headers headers = exchange.getResponseHeaders();
            headers.add("Access-Control-Allow-Origin", "*");
            headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            headers.add("Access-Control-Allow-Headers", "Content-Type");
            // 解析请求的方法，看GET还是POST
            String requestMethod = exchange.getRequestMethod();
            // 注意判断要用equals方法而不是==啊，java的小坑（
            if (requestMethod.equals("GET")) {
                // 处理GET
                handleGetRequest(exchange);
            } else if (requestMethod.equals("POST")) {
                // 处理POST
                handlePostRequest(exchange);
            } else if (requestMethod.equals("OPTIONS")) {
                // 处理OPTIONS
                handleOptionsRequest(exchange);
            } else {
                // 其他请求返回405 Method Not Allowed
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }
    private static void handleGetRequest(HttpExchange exchange) throws IOException {
        // 响应头，因为是JSON通信
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        // 状态码为200，也就是status ok
        exchange.sendResponseHeaders(200, 0);
        // 获取输出流，java用流对象来进行io操作
        OutputStream outputStream = exchange.getResponseBody();
        // 构建JSON响应数据，这里简化为字符串
        // 这里写的一个固定的JSON，实际可以查表获取数据，然后再拼出想要的JSON
        String response = "[{\"id\": 1, \"name\": \"John Doe\", \"department\": \"Computer Science\", \"type\": \"Student\"}," +
                "{\"id\": 2, \"name\": \"Jane Smith\", \"department\": \"Electrical Engineering\", \"type\": \"Faculty\"}]";
        // 写
        outputStream.write(response.getBytes());
        // 流一定要close！！！小心泄漏
        outputStream.close();
    }

    private static void handlePostRequest(HttpExchange exchange) throws IOException {
        // 读取POST请求体
        InputStream requestBody = exchange.getRequestBody();
        // 用这个请求体（输入流）构造个buffered reader
        BufferedReader reader = new BufferedReader(new InputStreamReader(requestBody));
        // 拼字符串的
        StringBuilder requestBodyBuilder = new StringBuilder();
        // 用来读的
        String line;
        // 没读完，一直读，拼到string builder里
        while ((line = reader.readLine()) != null) {
            requestBodyBuilder.append(line);
        }

        // 看看读到了啥
        // 实际处理可能会更复杂点
        System.out.println("Received POST request to create card with data: " + requestBodyBuilder.toString());

        // 响应头
        exchange.getResponseHeaders().set("Content-Type", "text/plain");
        // 响应状态码200
        exchange.sendResponseHeaders(200, 0);

        // 剩下三个和GET一样
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write("Card created successfully".getBytes());
        outputStream.close();
    }

    private static void handleOptionsRequest(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(204, -1);
    }

}
