package alien.servlets;

import alien.api.TomcatServer;
import alien.config.ConfigUtils;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@WebServlet("/.well-known/openid-configuration")
public class OpenIdConfigurationServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String jsonData = "{\"jwks_uri\":\"https://" +
                ConfigUtils.getLocalHostname() + ":" +
                TomcatServer.getPort() +
                "/jwk\"}";

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(jsonData);
    }
}
