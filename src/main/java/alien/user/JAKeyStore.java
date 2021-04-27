package alien.user;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import alien.api.Dispatcher;
import alien.api.token.GetTokenCertificate;
import alien.api.token.TokenCertificateType;
import alien.catalogue.CatalogueUtils;
import alien.config.ConfigUtils;
import lazyj.ExtProperties;
import lazyj.Format;

/**
 *
 * @author ron
 * @since Jun 22, 2011
 */
public class JAKeyStore {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(CatalogueUtils.class.getCanonicalName());

	/**
	 * length for the password generator
	 */
	private static final int passLength = 30;

	/**
	 *
	 */
	private static KeyStore clientCert = null;

	/**
	 *
	 */
	public static KeyStore tokenCert = null;

	/**
	 * Token can be stored as a string in environment
	 */
	private static String tokenCertString = null;
	private static String tokenKeyString = null;

	/**
	 *
	 */
	private static KeyStore hostCert = null;

	/**
	 *
	 */
	public static final KeyStore trustStore;

	private static final String charString = "!0123456789abcdefghijklmnopqrstuvwxyz@#$%^&*()-+=_{}[]:;|?/>.,<";

	/**
	 *
	 */
	public static final char[] pass = getRandomString();

	/**
	 *
	 */
	public static TrustManager trusts[];

	private static final int MAX_PASSWORD_RETRIES = 3;

	static {
		Security.addProvider(new BouncyCastleProvider());
		
		KeyStore ktmp = null;
		
		try {
			ktmp = KeyStore.getInstance("JKS");
			ktmp.load(null, pass);
			loadTrusts(ktmp);
		}
		catch (final KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
			logger.log(Level.SEVERE, "Exception during loading trust stores (static block)", e);
			e.printStackTrace();
		}
		
		trustStore = ktmp;
	}

	private static void loadTrusts(final KeyStore keystore) {
		final String trustsDirSet = ConfigUtils.getConfig().gets("trusted.certificates.location",
				UserFactory.getUserHome() + System.getProperty("file.separator") + ".j" + System.getProperty("file.separator") + "trusts");

		try {
			final StringTokenizer st = new StringTokenizer(trustsDirSet, ":");

			// total number of certificates loaded from the ":" separated folder list in the above configuration/environment variable
			int iLoaded = 0;

			while (st.hasMoreTokens()) {
				final File trustsDir = new File(st.nextToken().trim());

				if (logger.isLoggable(Level.INFO))
					logger.log(Level.INFO, "Loading trusts from " + trustsDir.getAbsolutePath());

				final File[] dirContents;

				if (trustsDir.exists() && trustsDir.isDirectory() && (dirContents = trustsDir.listFiles()) != null) {
					final CertificateFactory cf = CertificateFactory.getInstance("X.509");

					for (final File trust : dirContents)
						if (trust.getName().endsWith("der") || trust.getName().endsWith(".0"))
							try (FileInputStream fis = new FileInputStream(trust)) {
								final X509Certificate c = (X509Certificate) cf.generateCertificate(fis);
								if (logger.isLoggable(Level.FINE))
									logger.log(Level.FINE, "Trusting now: " + c.getSubjectDN());

								keystore.setEntry(trust.getName().substring(0, trust.getName().lastIndexOf('.')), new KeyStore.TrustedCertificateEntry(c), null);

								iLoaded++;
							}
							catch (final Exception e) {
								e.printStackTrace();
							}

					if (iLoaded == 0)
						logger.log(Level.WARNING, "No CA files found in " + trustsDir.getAbsolutePath());
					else
						logger.log(Level.INFO, "Loaded " + iLoaded + " certificates from " + trustsDir.getAbsolutePath());
				}
			}

			if (iLoaded == 0)
				try (InputStream classpathTrusts = JAKeyStore.class.getClassLoader().getResourceAsStream("trusted_authorities.jks")) {
					keystore.load(classpathTrusts, "castore".toCharArray());
					logger.log(Level.WARNING, "Found " + keystore.size() + " default trusted CAs in classpath");
				}
				catch (final Throwable t) {
					logger.log(Level.SEVERE, "Cannot load the default trust keystore from classpath", t);
				}

			if (keystore.equals(trustStore)) {
				final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
				tmf.init(trustStore);
				trusts = tmf.getTrustManagers();
			}
		}
		catch (final KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
			logger.log(Level.WARNING, "Exception during loading trust stores", e);
		}
	}

	/**
	 * Check file permissions of certificate and key
	 *
	 * @param user_key
	 *            path to key
	 */
	private static boolean checkKeyPermissions(final String user_key) {
		File key = new File(user_key);

		try {
			if (!user_key.equals(key.getCanonicalPath()))
				key = new File(key.getCanonicalPath());

			if (key.exists() && key.canRead()) {
				final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(key.toPath());

				boolean anyChange = false;

				for (final PosixFilePermission toRemove : EnumSet.range(PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_EXECUTE))
					if (permissions.remove(toRemove))
						anyChange = true;

				if (anyChange) {
					Files.setPosixFilePermissions(key.toPath(), permissions);
				}

				return true;
			}
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Error checking or modifying permissions on " + user_key, e);
		}

		return false;
	}

	private static boolean isEncrypted(final String path) {
		boolean encrypted = false;
		try (Scanner scanner = new Scanner(new File(path))) {
			while (scanner.hasNext()) {
				final String nextToken = scanner.next();
				if (nextToken.contains("ENCRYPTED")) {
					encrypted = true;
					break;
				}
			}
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			encrypted = false;
		}

		return encrypted;
	}

	/**
	 * @return initialize the client key storage (the full grid certificate)
	 */
	public static String getClientKeyPath() {
		final String defaultKeyPath = Paths.get(UserFactory.getUserHome(), ".globus", "userkey.pem").toString();
		final String user_key = selectPath("X509_USER_KEY", "user.cert.priv.location", defaultKeyPath);
		return user_key;
	}

	/**
	 * @return get the default location of the client certificate
	 */
	public static String getClientCertPath() {
		final String defaultCertPath = Paths.get(UserFactory.getUserHome(), ".globus", "usercert.pem").toString();
		final String user_cert = selectPath("X509_USER_CERT", "user.cert.pub.location", defaultCertPath);
		return user_cert;
	}

	private static boolean loadClientKeyStorage() {
		final String user_key = getClientKeyPath();
		final String user_cert = getClientCertPath();

		if (user_key == null || user_cert == null) {
			return false;
		}

		if (!checkKeyPermissions(user_key)) {
			logger.log(Level.WARNING, "Permissions on usercert.pem or userkey.pem are not OK");
			return false;
		}

		clientCert = makeKeyStore(user_key, user_cert, "USER CERT");
		return clientCert != null;
	}

	/**
	 * @param keypath
	 *            to the private key in order to test if the password is vaid
	 * @return char[] containing the correct password or empty string if the key is not encrypted
	 */
	public static char[] requestPassword(final String keypath) {
		PrivateKey key = null;
		char[] passwd = null;

		if (!isEncrypted(keypath))
			return "".toCharArray();

		for (int i = 0; i < MAX_PASSWORD_RETRIES; i++) {
			try {
				passwd = System.console().readPassword("Enter the password for " + keypath + ": ");
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				try (Scanner scanner = new Scanner(System.in)) {
					passwd = scanner.nextLine().toCharArray();
				}
			}

			try {
				key = loadPrivX509(keypath, passwd);
			}
			catch (@SuppressWarnings("unused") final org.bouncycastle.openssl.PEMException | org.bouncycastle.pkcs.PKCSException e) {
				logger.log(Level.WARNING, "Failed to load key " + keypath + ", most probably wrong password.");
				System.out.println("Wrong password! Try again");
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				logger.log(Level.WARNING, "Failed to load key " + keypath);
				System.out.println("Failed to load key");
				break;
			}

			if (key != null)
				break;
		}

		return passwd;
	}

	/**
	 * @param certString
	 *            programatically set the token certificate
	 * @param keyString
	 *            programatically set the token key
	 * @throws Exception
	 *             if something goes wrong
	 */
	public static void createTokenFromString(final String certString, final String keyString) throws Exception {
		tokenCertString = certString;
		tokenKeyString = keyString;

		loadKeyStore();
	}

	/**
	 * @param var
	 *            environment variable to be checked
	 * @param key
	 *            in configuration to be checked
	 * @param fsPath
	 *            the filesystem path, usually the fallback/default location
	 * @return path selected from one of the three provided locations
	 */
	public static String selectPath(final String var, final String key, final String fsPath) {
		final ExtProperties config = ConfigUtils.getConfig();

		if (var != null && System.getenv(var) != null) {
			return System.getenv(var);
		}
		else if (key != null && config.gets(key) != null && !config.gets(key).isEmpty()) {
			return config.gets(key);
		}
		else if (fsPath != null && !fsPath.isEmpty() && Files.exists(Paths.get(fsPath))) {
			return fsPath;
		}
		else {
			return null;
		}
	}

	private static KeyStore makeKeyStore(final String key, final String cert, final String message) {
		if (key == null || cert == null)
			return null;

		KeyStore ks = null;
		logger.log(Level.SEVERE, "Trying to load " + message);

		try {
			ks = KeyStore.getInstance("JKS");
			ks.load(null, pass);
			loadTrusts(ks);

			addKeyPairToKeyStore(ks, "User.cert", key, cert);
			logger.log(Level.SEVERE, "Loaded " + message);
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Error loading " + message, e);
			ks = null;
		}

		return ks;
	}

	/**
	 * Load the token credentials (required for running Tomcat / WebSockets)
	 *
	 * @return <code>true</code> if token has been successfully loaded
	 */
	public static boolean loadTokenKeyStorage() {
		final String sUserId = UserFactory.getUserID();
		final String tmpDir = System.getProperty("java.io.tmpdir");

		if (sUserId == null && (tokenKeyString == null || tokenCertString == null))
			logger.log(Level.SEVERE, "Cannot get the current user's ID");

		final String tokenKeyFilename = "tokenkey_" + sUserId + ".pem";
		final String defaultTokenKeyPath = Paths.get(tmpDir, tokenKeyFilename).toString();
		final String token_key;

		if (tokenKeyString != null) {
			token_key = tokenKeyString;
		}
		else {
			token_key = selectPath("JALIEN_TOKEN_KEY", "tokenkey.path", defaultTokenKeyPath);
		}

		final String tokenCertFilename = "tokencert_" + sUserId + ".pem";
		final String defaultTokenCertPath = Paths.get(tmpDir, tokenCertFilename).toString();
		final String token_cert;

		if (tokenCertString != null) {
			token_cert = tokenCertString;
		}
		else {
			token_cert = selectPath("JALIEN_TOKEN_CERT", "tokencert.path", defaultTokenCertPath);
		}

		tokenCert = makeKeyStore(token_key, token_cert, "TOKEN CERT");
		return tokenCert != null;
	}

	/**
	 * @return <code>true</code> if keystore is loaded successfully
	 */
	private static boolean loadServerKeyStorage() {
		final String defaultKeyPath = Paths.get(UserFactory.getUserHome(), ".globus", "hostkey.pem").toString();
		final String host_key = selectPath(null, "host.cert.priv.location", defaultKeyPath);

		final String defaultCertPath = Paths.get(UserFactory.getUserHome(), ".globus", "hostcert.pem").toString();
		final String host_cert = selectPath(null, "host.cert.pub.location", defaultCertPath);

		hostCert = makeKeyStore(host_key, host_cert, "HOST CERT");
		return hostCert != null;
	}

	private static void addKeyPairToKeyStore(final KeyStore ks, final String entryBaseName, final String privKeyLocation, final String pubKeyLocation) throws Exception {
		final char[] passwd = requestPassword(privKeyLocation);
		if (passwd == null)
			throw new Exception("Failed to read password for key " + privKeyLocation);

		final PrivateKey key = loadPrivX509(privKeyLocation, passwd);
		final X509Certificate[] certChain = loadPubX509(pubKeyLocation, true);
		final PrivateKeyEntry entry = new PrivateKeyEntry(key, certChain);

		ks.setEntry(entryBaseName, entry, new PasswordProtection(pass));
	}

	/**
	 * @param ks
	 * @param filename
	 * @param password
	 * @return <code>true</code> if the keystore was successfully saved to the target path, <code>false</code> if not
	 */
	public static boolean saveKeyStore(final KeyStore ks, final String filename, final char[] password) {
		if (ks == null) {
			logger.log(Level.WARNING, "Null key store to write to " + filename);
			return false;
		}

		final File f = new File(filename);

		try (FileOutputStream fo = new FileOutputStream(f)) {
			final Set<PosixFilePermission> attrs = new HashSet<>();
			attrs.add(PosixFilePermission.OWNER_READ);
			attrs.add(PosixFilePermission.OWNER_WRITE);

			try {
				Files.setPosixFilePermissions(f.toPath(), attrs);
			}
			catch (final IOException io2) {
				logger.log(Level.WARNING, "Could not protect your keystore " + filename + " with POSIX attributes", io2);
			}

			try {
				ks.store(fo, password);
				f.deleteOnExit();
			}
			catch (final KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
				e.printStackTrace();
			}

			return true;
		}
		catch (final IOException e1) {
			logger.log(Level.WARNING, "Exception saving key store", e1);
		}

		return false;
	}

	/**
	 * @param keyFileLocation
	 * @param password
	 * @return priv key
	 * @throws IOException
	 * @throws PEMException
	 * @throws OperatorCreationException
	 * @throws PKCSException
	 */
	public static PrivateKey loadPrivX509(final String keyFileLocation, final char[] password) throws IOException, PEMException, OperatorCreationException, PKCSException {

		if (logger.isLoggable(Level.INFO))
			logger.log(Level.INFO, "Loading private key: " + keyFileLocation);

		Reader source = null;
		try {
			source = new FileReader(keyFileLocation);
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			source = new StringReader(keyFileLocation);
		}

		try (PEMParser reader = new PEMParser(new BufferedReader(source))) {
			Object obj;
			while ((obj = reader.readObject()) != null) {
				if (obj instanceof PEMEncryptedKeyPair) {
					final PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(password);
					final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

					final KeyPair kp = converter.getKeyPair(((PEMEncryptedKeyPair) obj).decryptKeyPair(decProv));

					return kp.getPrivate();
				}

				if (obj instanceof PEMKeyPair)
					obj = ((PEMKeyPair) obj).getPrivateKeyInfo();
				// and let if fall through the next case

				if (obj instanceof PKCS8EncryptedPrivateKeyInfo) {
					final InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password);

					obj = ((PKCS8EncryptedPrivateKeyInfo) obj).decryptPrivateKeyInfo(pkcs8Prov);
				}

				if (obj instanceof PrivateKeyInfo) {
					final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

					return converter.getPrivateKey(((PrivateKeyInfo) obj));
				}

				if (obj instanceof PrivateKey)
					return (PrivateKey) obj;

				if (obj instanceof KeyPair)
					return ((KeyPair) obj).getPrivate();

				System.err.println("Unknown object type: " + obj + "\n" + obj.getClass().getCanonicalName());
			}

			return null;
		}
	}

	/**
	 * @param certFileLocation
	 * @param checkValidity
	 * @return Cert chain
	 */
	public static X509Certificate[] loadPubX509(final String certFileLocation, final boolean checkValidity) {

		if (logger.isLoggable(Level.INFO))
			logger.log(Level.INFO, "Loading public key: " + certFileLocation);

		Reader source = null;
		try {
			source = new FileReader(certFileLocation);
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			source = new StringReader(certFileLocation);
		}

		try (PEMParser reader = new PEMParser(new BufferedReader(source))) {
			Object obj;

			final ArrayList<X509Certificate> chain = new ArrayList<>();

			while ((obj = reader.readObject()) != null)
				if (obj instanceof X509Certificate) {
					final X509Certificate c = (X509Certificate) obj;
					try {
						c.checkValidity();
					}
					catch (final CertificateException e) {
						logger.log(Level.SEVERE, "Your certificate has expired or is invalid!", e);
						System.err.println("Your certificate has expired or is invalid:\n  " + e.getMessage());
						reader.close();
						return null;
					}
					chain.add(c);
				}
				else if (obj instanceof X509CertificateHolder) {
					final X509CertificateHolder ch = (X509CertificateHolder) obj;

					try {
						final X509Certificate c = new JcaX509CertificateConverter().setProvider("BC").getCertificate(ch);

						if (checkValidity)
							c.checkValidity();

						chain.add(c);
					}
					catch (final CertificateException ce) {
						logger.log(Level.SEVERE, "Exception loading certificate", ce);
					}
				}
				else
					System.err.println("Unknown object type: " + obj + "\n" + obj.getClass().getCanonicalName());

			if (chain.size() > 0)
				return chain.toArray(new X509Certificate[0]);

			return null;
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * @return randomized char array of passLength length
	 */
	public static char[] getRandomString() {
		final StringBuffer s = new StringBuffer(passLength);
		for (int i = 0; i < passLength; i++) {
			final int pos = ThreadLocalRandom.current().nextInt(charString.length());

			s.append(charString.charAt(pos));
		}
		return s.toString().toCharArray();
	}

	private static boolean keystore_loaded = false;

	/**
	 * @return <code>true</code> if JAliEn managed to load one of keystores
	 */
	public static boolean loadKeyStore() {
		keystore_loaded = false;

		// If JALIEN_TOKEN_CERT env var is set, token is in highest priority
		if (System.getenv("JALIEN_TOKEN_CERT") != null || tokenCertString != null) {
			keystore_loaded = loadTokenKeyStorage();
		}

		if (!keystore_loaded)
			keystore_loaded = loadClientKeyStorage();
		if (!keystore_loaded)
			keystore_loaded = loadServerKeyStorage();
		if (!keystore_loaded)
			keystore_loaded = loadTokenKeyStorage();

		if (!keystore_loaded) {
			final String msg = "Failed to load any certificate, tried: user, host and token";
			logger.log(Level.SEVERE, msg);
			System.err.println("ERROR: " + msg);
		}

		return keystore_loaded;
	}

	/**
	 * @return either tokenCert, clientCert or hostCert keystore
	 */
	public static KeyStore getKeyStore() {
		if (!keystore_loaded)
			loadKeyStore();

		if ((System.getenv("JALIEN_TOKEN_CERT") != null || tokenCertString != null) && (JAKeyStore.tokenCert != null)) {
			return JAKeyStore.tokenCert;
		}

		if (JAKeyStore.clientCert != null) {
			try {
				if (clientCert.getCertificateChain("User.cert") == null) {
					loadKeyStore();
				}
			}
			catch (final KeyStoreException e) {
				logger.log(Level.SEVERE, "Exception during loading client cert");
				e.printStackTrace();
			}
			return JAKeyStore.clientCert;
		}
		else if (JAKeyStore.hostCert != null) {
			try {
				if (hostCert.getCertificateChain("User.cert") == null)
					loadKeyStore();
			}
			catch (final KeyStoreException e) {
				logger.log(Level.SEVERE, "Exception during loading host cert");
				e.printStackTrace();
			}
			return JAKeyStore.hostCert;
		}
		else if (JAKeyStore.tokenCert != null) {
			try {
				if (tokenCert.getCertificateChain("User.cert") == null)
					loadKeyStore();
			}
			catch (final KeyStoreException e) {
				logger.log(Level.SEVERE, "Exception during loading token cert");
				e.printStackTrace();
			}
			return JAKeyStore.tokenCert;
		}

		return null;
	}

	/**
	 * Request token certificate from JCentral
	 *
	 * @return <code>true</code> if tokencert was successfully received
	 */
	public static boolean requestTokenCert() {
		// Get user certificate to connect to JCentral
		Certificate[] cert = null;
		AliEnPrincipal userIdentity = null;
		try {
			cert = JAKeyStore.getKeyStore().getCertificateChain("User.cert");
			if (cert == null) {
				logger.log(Level.SEVERE, "Failed to load certificate");
				return false;
			}
		}
		catch (final KeyStoreException e) {
			e.printStackTrace();
		}

		if (cert instanceof X509Certificate[]) {
			final X509Certificate[] x509cert = (X509Certificate[]) cert;
			userIdentity = UserFactory.getByCertificate(x509cert);
		}
		if (userIdentity == null) {
			logger.log(Level.SEVERE, "Failed to get user identity");
			return false;
		}

		final String sUserId = UserFactory.getUserID();

		if (sUserId == null) {
			logger.log(Level.SEVERE, "Cannot get the current user's ID");
			return false;
		}

		// Two files will be the result of this command
		// Check if their location is set by env variables or in config, otherwise put default location in $TMPDIR/
		final String tmpDir = System.getProperty("java.io.tmpdir");
		final String defaultTokenKeyPath = Paths.get(tmpDir, "tokenkey_" + sUserId + ".pem").toString();
		final String defaultTokenCertPath = Paths.get(tmpDir, "tokencert_" + sUserId + ".pem").toString();

		String tokencertpath = selectPath("JALIEN_TOKEN_CERT", "tokencert.path", defaultTokenCertPath);
		String tokenkeypath = selectPath("JALIEN_TOKEN_KEY", "tokenkey.path", defaultTokenKeyPath);

		if (tokencertpath == null)
			tokencertpath = defaultTokenCertPath;

		if (tokenkeypath == null)
			tokenkeypath = defaultTokenKeyPath;

		final GetTokenCertificate tokRequest = new GetTokenCertificate(userIdentity, userIdentity.getDefaultUser(), TokenCertificateType.USER_CERTIFICATE, null,
				TokenCertificateType.USER_CERTIFICATE.getMaxValidity());

		final GetTokenCertificate tokReply;

		try {
			tokReply = Dispatcher.execute(tokRequest);
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Token request failed", e);
			return false;
		}

		new File(tokencertpath).delete();
		new File(tokenkeypath).delete();

		try ( // Open files for writing
				PrintWriter pwritercert = new PrintWriter(new File(tokencertpath));
				PrintWriter pwriterkey = new PrintWriter(new File(tokenkeypath))) {

			// Set correct permissions
			Files.setPosixFilePermissions(Paths.get(tokencertpath), PosixFilePermissions.fromString("r--r-----"));
			Files.setPosixFilePermissions(Paths.get(tokenkeypath), PosixFilePermissions.fromString("r--------"));

			pwritercert.write(tokReply.getCertificateAsString());
			pwriterkey.write(tokReply.getPrivateKeyAsString());
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Exception writing token content to files", e);
			return false;
		}

		return true;
	}

	/**
	 * @param ksName which keystore to check
	 * @return <code>true</code> if the requested certificate has been successfully loaded
	 */
	public static boolean isLoaded(final String ksName) {
		final KeyStore ks;

		switch (ksName) {
			case "user":
				ks = clientCert;
				break;
			case "host":
				ks = hostCert;
				break;
			case "token":
				ks = tokenCert;
				break;
			default:
				ks = null;
		}

		return isLoaded(ks);
	}

	private static boolean isLoaded(final KeyStore ks) {
		boolean status = false;
		if (ks != null) {
			try {
				status = ks.getCertificateChain("User.cert") != null;
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				// Do nothing
			}
		}
		return status;
	}

	/**
	 * Starts a thread in the background that will update the token every two hours
	 */
	public static void startTokenUpdater() {
		// Refresh token cert every two hours
		new Thread() {
			@Override
			public void run() {
				try {
					while (true) {
						sleep(2 * 60 * 60 * 1000);
						JAKeyStore.requestTokenCert();
					}
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	/**
	 * Fetch and load the first token that will be used for Tomcat
	 *
	 * @return <code>true</code> if the token is fetched and loaded successfully
	 */
	public static boolean bootstrapFirstToken() {
		if (!JAKeyStore.requestTokenCert()) {
			return false;
		}

		try {
			if (!JAKeyStore.loadTokenKeyStorage()) {
				System.err.println("Token Certificate could not be loaded.");
				System.err.println("Exiting...");
				return false;
			}
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Error loading token", e);
			System.err.println("Error loading token");
			return false;
		}

		return true;
	}

	/**
	 * Get certificate's expiration date as long value
	 *
	 * @param ks a keystore that contains the certificate
	 * @return epoch time of certificate's not-valid-after field
	 */
	public static long getExpirationTime(final KeyStore ks) {
		Certificate c;
		try {
			c = ks.getCertificateChain("User.cert")[0];
			final long endTime = ((X509Certificate) c).getNotAfter().getTime();
			return endTime;
		}
		catch (final KeyStoreException e) {
			e.printStackTrace();
		}

		return 0;
	}

	/**
	 * Check if the certificate will expire in the next two days
	 *
	 * @param endTime expiration time of the certificate
	 *
	 * @return <code>true</code> if the certificate will be valid for less than two days
	 */
	public static boolean expireSoon(final long endTime) {
		return endTime - System.currentTimeMillis() < 1000L * 60 * 60 * 24 * 2;
	}

	/**
	 * Print to stdout how many days, hours and minutes left for the certificate to expire
	 *
	 * @param endTime certificate's getNotAfter() time
	 */
	public static void printExpirationTime(final long endTime) {
		final long now = System.currentTimeMillis();

		if (endTime < now) {
			System.err.println("> Your certificate has expired on " + (new Date(endTime)));
			return;
		}

		System.err.println("> Your certificate will expire in " + Format.toInterval(endTime - now));
	}
}
