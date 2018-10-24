package ru.mail.polis.shah;

import kotlin.text.Charsets;
import one.nio.config.ConfigParser;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class KVServiceImpl extends HttpServer implements KVService {

    private final KVDao dao;
    private final Logger logger;

    public KVServiceImpl(KVDao dao, int port) throws IOException {
        super(KVServiceImpl.createConfig(port));
        this.dao = dao;
        this.logger = LoggerFactory.getLogger(KVService.class);
    }

    private static HttpServerConfig createConfig(int port) {
        InputStream configStream = KVServiceImpl.class.getClassLoader().getResourceAsStream("server_config.yaml");
        String config = new BufferedReader(new InputStreamReader(configStream, Charsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        config = String.format(config, port);
        return ConfigParser.parse(config, HttpServerConfig.class);
    }

    @Override
    public void start() {
        super.start();
    }

    @Path("/v0/status")
    public Response status(Request request) {
        return Response.ok(Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(Request request) {
        String key = request.getParameter("id=");
        if (key == null || key.length() == 0) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        byte[] keyb = key.getBytes();
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    try {
                        byte[] result = dao.get(keyb);
                        return Response.ok(result);
                    } catch (NoSuchElementException ignore) {
                        return new Response(Response.NOT_FOUND, Response.EMPTY);
                    }
                case Request.METHOD_PUT:
                    byte[] value = request.getBody();
                    dao.upsert(keyb, value);
                    return new Response(Response.CREATED, Response.EMPTY);
                case Request.METHOD_DELETE:
                    dao.remove(keyb);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
        return new Response(Response.NOT_IMPLEMENTED, Response.EMPTY);
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }
}
