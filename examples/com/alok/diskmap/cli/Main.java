package com.alok.diskmap.cli;

import com.alok.diskmap.DiskBackedMap;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import javax.servlet.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        Context root = new Context(server,"/", Context.SESSIONS);
        root.addServlet(new ServletHolder(new HelloServlet()), "/*");
        server.start();

    }

    public static class HelloServlet extends HttpServlet {

        @Override
        public void init(ServletConfig servletConfig) throws ServletException {
            File tempFile = null;
            try {
                tempFile = File.createTempFile("foo", "tmp");
                servletConfig.getServletContext().setAttribute("storage", new DiskBackedMap(System.getProperty("java.io.tmpdir")));
            } catch (IOException e) {
                e.printStackTrace();
                throw new ServletException("Cannot create temp file", e);
            }
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String uri = req.getRequestURI();
            String key = "";
            String value = "";
            if(uri.indexOf("/map/") > -1){
                key = uri.substring(uri.indexOf("/map/") + 5);
                Map map = (Map) req.getSession().getServletContext().getAttribute("storage");
                value = (String) map.get(key);

            }
            resp.getWriter().format("<html><body><form action='/' method='post'>");
            resp.getWriter().format("<label for='key' value='Key'><input type=text id=key name='key' value='%s'></label>", key);
            resp.getWriter().format("<label for='value' value='Value'><input type=text id=value name='value' value='%s'></label>", value);
            resp.getWriter().print("<input type=submit></form></body><html>");
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String key = req.getParameter("key");
            String value = req.getParameter("value");
            Map map = (Map) req.getSession().getServletContext().getAttribute("storage");
            map.put(key, value);
            resp.getWriter().format("<html><body>ok</body></html>");
        }
    }

}
