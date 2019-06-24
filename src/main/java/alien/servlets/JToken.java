package alien.servlets;

import java.io.IOException;
import java.security.cert.X509Certificate;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.token.GetTokenCertificate;
import alien.api.token.TokenCertificateType;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.taskQueue.JobToken;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import lazyj.RequestWrapper;

/**
 * Generate job tokens for jAliEn jobs to authenticate with
 *
 * @author costing
 * @since 2018-04-13
 */
public class JToken extends HttpServlet {

	private static final AliEnPrincipal requester = UserFactory.getByUsername("jobagent");

	static {
		final X509Certificate cert = GetTokenCertificate.getRootPublicKey();

		if (cert != null)
			requester.setUserCert(new X509Certificate[] { cert });
	}

	/**
	 * Monitor object to report statistics with
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(JToken.class.getCanonicalName());

	/**
	 *
	 */
	private static final long serialVersionUID = 11942488126789341L;

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		final Timing timing = new Timing();

		try {
			final RequestWrapper rw = new RequestWrapper(req);

			final long queueId = rw.getl("queueId", -1);

			if (queueId < 0) {
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "You need to pass a valid queueId URL parameter");
				return;
			}

			final String username = rw.gets("username").trim();

			if (username == null || username.length() == 0) {
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "You need to pass a valid username URL parameter");
				return;
			}

			final int resubmission = rw.geti("resubmission", 0);

			try (ServletOutputStream os = resp.getOutputStream()) {
				GetTokenCertificate gtc = new GetTokenCertificate(requester, username, TokenCertificateType.JOB_TOKEN, "queueid=" + queueId + "/resubmission=" + resubmission, 1);
				try {
					gtc = Dispatcher.execute(gtc);

					os.println("$VAR1 = [");
					os.println("  {");
					os.println("    'publicKey' => '" + gtc.getCertificateAsString() + "',");
					os.println("    'privateKey' => '" + gtc.getPrivateKeyAsString() + "',");
					os.println("    'token' => '" + JobToken.generateToken() + "'");
					os.println("  }");
					os.println("];");
				} catch (final ServerException e) {
					resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Exception executing the request: " + e.getMessage());
				}
			}
		} finally {
			if (monitor != null)
				monitor.addMeasurement("ms_to_answer", timing);
		}
	}
}
