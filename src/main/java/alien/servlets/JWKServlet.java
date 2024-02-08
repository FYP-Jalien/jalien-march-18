package alien.servlets;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import alien.user.UserFactory;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

@WebServlet("/jwk")
public class JWKServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String JAuthZPubLocation = ConfigUtils.getConfig().gets("jAuthZ.pub.key.location", UserFactory.getUserHome() +
                FileSystems.getDefault().getSeparator() + ".alien" +
                FileSystems.getDefault().getSeparator() + "authen" +
                FileSystems.getDefault().getSeparator() + "lpub.pem");

        String jsonData = "{\"keys\":[{\n" +
                "    \"kty\": \"RSA\",\n" +
                "    \"use\": \"sig\",\n" +
                "    \"e\": \"AQAB\",\n" +
                "    \"kid\": \"ALICE\",\n" +
                "    \"n\": \"" +
                Base64.getUrlEncoder().encodeToString(Arrays.toString(Objects.requireNonNull(JAKeyStore.loadPubX509(JAuthZPubLocation, false))).getBytes()) + "\",\n" +
                "    \"alg\": \"RS256\"\n" +
                "}]}";

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(jsonData);
    }
}
