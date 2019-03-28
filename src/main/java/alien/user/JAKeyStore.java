package alien.user;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
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

import alien.catalogue.CatalogueUtils;
import alien.config.ConfigUtils;
import lazyj.ExtProperties;
import lazyj.commands.SystemCommand;

/**
 *
 * @author ron
 * @since Jun 22, 2011
 */
public class JAKeyStore {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(CatalogueUtils.class.getCanonicalName());

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
	public static KeyStore trustStore;

	/**
	 *
	 */
	public static final char[] pass = getRandomString();

	/**
	 *
	 */
	public static TrustManager trusts[];

	static {
		Security.addProvider(new BouncyCastleProvider());
		try {
			trustStore = KeyStore.getInstance("JKS");
			trustStore.load(null, pass);
			loadTrusts(trustStore);
		} catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
			logger.log(Level.SEVERE, "Exception during loading trust stores (static block)", e);
			e.printStackTrace();
		}

	}

	private static void loadTrusts(final KeyStore keystore) {
		final String trustsDirSet = ConfigUtils.getConfig().gets("trusted.certificates.location",
				System.getProperty("user.home") + System.getProperty("file.separator") + ".j" + System.getProperty("file.separator") + "trusts");

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
							} catch (final Exception e) {
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
				} catch (final Throwable t) {
					logger.log(Level.SEVERE, "Cannot load the default trust keystore from classpath", t);
				}

			if (keystore.equals(trustStore)) {
				final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
				tmf.init(trustStore);
				trusts = tmf.getTrustManagers();
			}
		} catch (final KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
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
		} catch (final IOException e) {
			logger.log(Level.WARNING, "Error checking or modifying permissions on " + user_key, e);
		}

		return false;
	}

	/**
	 * @return true if ok
	 * @throws Exception
	 */
	private static boolean loadClientKeyStorage() throws Exception {
		// return loadClientKeyStorage(false);
		// return loadClientKeyStorage(true);
		// String proxy = System.getenv().get("X509_USER_PROXY");
		// if (proxy != null) {
		// System.out.println("Using proxy");
		// return loadProxy();
		// }
		// System.out.println("Using certificates");
		return loadClientKeyStorage(false);

	}

	/**
	 * EXPERIMENTAL
	 *
	 * @return <code>true</code> if the default proxy could be loaded.
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	private static boolean loadProxy() throws Exception {
		String sUserId = System.getProperty("userid");
		if (sUserId == null || sUserId.length() == 0) {
			sUserId = SystemCommand.bash("id -u " + System.getProperty("user.name")).stdout;

			if (sUserId != null && sUserId.length() > 0)
				System.setProperty("userid", sUserId);
			else {
				logger.severe("User Id empty! Could not get the token file name");
				return false;
			}
		}
		final String proxyLocation = "/tmp/x509up_u" + sUserId;

		// load pair
		// =================
		class PkiUtils {
			// public static List<?> readPemObjects(InputStream is, final String pphrase)
			public List<Object> readPemObjects(final InputStream is, final String pphrase) throws IOException {
				final List<Object> list = new LinkedList<>();
				try (final PEMParser pr2 = new PEMParser(new InputStreamReader(is))) {
					final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
					final JcaX509CertificateConverter certconv = new JcaX509CertificateConverter().setProvider("BC");

					while (true) {
						final Object o = pr2.readObject();
						if (null == o)
							break; // done

						list.add(parsePemObject(o, pphrase, converter, certconv));
					}
				}
				return list;
			}

			private Object parsePemObject(final Object param, final String pphrase, final JcaPEMKeyConverter converter, final JcaX509CertificateConverter certconv) {
				Object o = param;

				try {
					if (o instanceof PEMEncryptedKeyPair) {
						o = ((PEMEncryptedKeyPair) o).decryptKeyPair(new JcePEMDecryptorProviderBuilder().build(pphrase.toCharArray()));
					}
					else
						if (o instanceof PKCS8EncryptedPrivateKeyInfo) {
							final InputDecryptorProvider pkcs8decoder = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(pphrase.toCharArray());
							o = converter.getPrivateKey(((PKCS8EncryptedPrivateKeyInfo) o).decryptPrivateKeyInfo(pkcs8decoder));
						}
				} catch (final Throwable t) {
					throw new RuntimeException("Failed to decode private key", t);
				}

				if (o instanceof PEMKeyPair) {
					try {
						return converter.getKeyPair((PEMKeyPair) o);
					} catch (final PEMException e) {
						throw new RuntimeException("Failed to construct public/private key pair", e);
					}
				}
				/*
				 * else if(o instanceof RSAPrivateCrtKey){ RSAPrivateCrtKey pk =
				 * (RSAPrivateCrtKey) o;
				 * System.err.println("=========== private key cert =========="); //return
				 * makeKeyPair(pk); return null; }
				 */
				else
					if (o instanceof X509CertificateHolder) {
						try {
							return certconv.getCertificate((X509CertificateHolder) o);
						} catch (final Exception e) {
							throw new RuntimeException("Failed to read X509 certificate", e);
						}
					}
					else {
						// catchsink, should check for certs and reject rest?
						System.out.println("generic case  type " + o.getClass().getName());
						return o;
					}
			}
		}
		// =================
		clientCert = KeyStore.getInstance("JKS");
		try {
			clientCert.load(null, pass);
		} catch (final Exception e) {
			// ignore
		}

		try (final FileInputStream proxyIS = new FileInputStream(proxyLocation)) {
			final List<Object> l = (new PkiUtils()).readPemObjects(proxyIS, "");
			final KeyPair kp = (KeyPair) l.get(1);
			final ArrayList<X509Certificate> x509l = new ArrayList<>();
			for (final Object o : l) {
				// System.out.println(o);
				if (!(o instanceof KeyPair)) {
					x509l.add((X509Certificate) o);
				}
			}
			addKeyPairToKeyStore(clientCert, "User.cert", kp, x509l);
		} catch (final FileNotFoundException e) {
			System.err.println("Proxy file not found");
		} catch (final IOException e) {
			System.err.println("Error while reading proxy file: " + e);
		}
		// get pair
		// call overloaded add
		return true;
	}

	/**
	 * @param noUserPass
	 * @return true if ok
	 * @throws Exception
	 */
	private static boolean loadClientKeyStorage(final boolean noUserPass) throws Exception {
		System.out.println("LOADING USER CERT");

		final ExtProperties config = ConfigUtils.getConfig();

		final String user_key = System.getenv("X509_USER_KEY") != null ? System.getenv("X509_USER_KEY")
				: config.gets("user.cert.priv.location", System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "userkey.pem");

		final String user_cert = System.getenv("X509_USER_CERT") != null ? System.getenv("X509_USER_CERT")
				: config.gets("user.cert.pub.location", System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "usercert.pem");

		if (!checkKeyPermissions(user_key)) {
			logger.log(Level.WARNING, "Permissions on usercert.pem or userkey.pem are not OK");
			return false;
		}

		clientCert = KeyStore.getInstance("JKS");

		JPasswordFinder jpf;

		if (noUserPass)
			jpf = new JPasswordFinder(new char[] {});
		else
			jpf = getPassword();

		clientCert.load(null, pass);
		loadTrusts(clientCert);

		addKeyPairToKeyStore(clientCert, "User.cert", user_key, user_cert, jpf);

		return true;
	}

	/**
	 * @param certString
	 * @param keyString
	 * @throws Exception
	 */
	public static void createTokenFromString(final String certString, final String keyString) throws Exception {
		tokenCertString = certString;
		tokenKeyString = keyString;

		loadKeyStore();
	}

	/**
	 * @return true if ok
	 * @throws Exception
	 */
	public static boolean loadTokenKeyStorage() throws Exception {
		final ExtProperties config = ConfigUtils.getConfig();
		final String sUserId = UserFactory.getUserID();

		if (sUserId == null) {
			logger.log(Level.SEVERE, "Cannot get the current user's ID");
			return false;
		}

		final int iUserId = Integer.parseInt(sUserId.trim());

		final String token_key;
		if (tokenKeyString != null)
			token_key = tokenKeyString;
		else
			if (System.getenv("JALIEN_TOKEN_KEY") != null)
				token_key = System.getenv("JALIEN_TOKEN_KEY");
			else
				token_key = config.gets("tokenkey.path", System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "tokenkey_" + iUserId + ".pem");

		final String token_cert;
		if (tokenCertString != null)
			token_cert = tokenCertString;
		else
			if (System.getenv("JALIEN_TOKEN_CERT") != null)
				token_cert = System.getenv("JALIEN_TOKEN_CERT");
			else
				token_cert = config.gets("tokencert.path", System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "tokencert_" + iUserId + ".pem");

		tokenCert = KeyStore.getInstance("JKS");

		try {
			tokenCert.load(null, pass);
			loadTrusts(tokenCert);

			addKeyPairToKeyStore(tokenCert, "User.cert", token_key, token_cert, null);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return false;
		}
		return true;

	}

	/**
	 * @throws Exception
	 */
	/*
	 * private static void loadPilotKeyStorage() throws Exception {
	 *
	 * final ExtProperties config = ConfigUtils.getConfig();
	 *
	 * clientCert = KeyStore.getInstance("JKS");
	 *
	 * try {
	 * // pass = getRandomString();
	 *
	 * clientCert.load(null, pass);
	 * loadTrusts();
	 *
	 * addKeyPairToKeyStore(clientCert, "User.cert",
	 * config.gets("host.cert.priv.location", System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "hostkey.pem"),
	 * config.gets("host.cert.pub.location", System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "hostcert.pem"),
	 * new JPasswordFinder(new char[] {}));
	 *
	 * } catch (final NoSuchAlgorithmException e) {
	 * e.printStackTrace();
	 * } catch (final CertificateException e) {
	 * e.printStackTrace();
	 * } catch (final IOException e) {
	 * e.printStackTrace();
	 * }
	 * }
	 */
	/**
	 * @return true if keystore is loaded successfully
	 * @throws Exception
	 */
	private static boolean loadServerKeyStorage() throws Exception {

		final ExtProperties config = ConfigUtils.getConfig();
		// pass = getRandomString();

		final String hostkey = config.gets("host.cert.priv.location",
				System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "hostkey.pem");

		final String hostcert = config.gets("host.cert.pub.location",
				System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "hostcert.pem");

		hostCert = KeyStore.getInstance("JKS");

		try {
			hostCert.load(null, pass);
			loadTrusts(hostCert);

			addKeyPairToKeyStore(hostCert, "User.cert", hostkey, hostcert, null);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return false;
		}

		return true;
	}

	private static JPasswordFinder getPassword() {

		final Console cons = System.console();
		Reader isr = null;
		if (cons == null)
			isr = new InputStreamReader(System.in);
		else {
			final char[] passwd = cons.readPassword("Grid certificate password: ");
			final String password = String.valueOf(passwd);
			isr = new StringReader(password);
		}

		final BufferedReader in = new BufferedReader(isr);

		try {
			final String line = in.readLine();

			if (line != null && line.length() > 0)
				return new JPasswordFinder(line.toCharArray());
		} catch (final IOException e) {
			logger.log(Level.WARNING, "Could not read passwd from System.in .", e);
		}

		return new JPasswordFinder("".toCharArray());
	}

	@SuppressWarnings("unused")
	private static void createKeyStore(final KeyStore ks, final String keyStoreName) {

		// pass = getRandomString();

		try (final FileInputStream f = new FileInputStream(keyStoreName)) {
			try {
				ks.load(null, pass);
			} catch (final NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (final CertificateException e) {
				e.printStackTrace();
			} catch (final IOException e) {
				e.printStackTrace();
			}
		} catch (final IOException e) {
			logger.log(Level.WARNING, "Exception creating key store", e);
		}
	}

	private static void addKeyPairToKeyStore(final KeyStore ks, final String entryBaseName, final String privKeyLocation, final String pubKeyLocation, final JPasswordFinder pFinder) throws Exception {
		ks.setEntry(entryBaseName, new KeyStore.PrivateKeyEntry(loadPrivX509(privKeyLocation, pFinder != null ? pFinder.getPassword() : null), loadPubX509(pubKeyLocation, true)),
				new KeyStore.PasswordProtection(pass));
	}

	private static void addKeyPairToKeyStore(final KeyStore ks, final String entryBaseName, final KeyPair pair, final ArrayList<X509Certificate> chain) throws Exception {

		X509Certificate[] certArray = new X509Certificate[chain.size()];
		certArray = chain.toArray(certArray);
		ks.setEntry(entryBaseName, new KeyStore.PrivateKeyEntry(pair.getPrivate(), certArray), new KeyStore.PasswordProtection(pass));
	}

	/**
	 * @param ks
	 * @param filename
	 * @param password
	 */
	public static void saveKeyStore(final KeyStore ks, final String filename, final char[] password) {
		try (final FileOutputStream fo = new FileOutputStream(filename)) {
			try {
				ks.store(fo, password);
			} catch (final KeyStoreException e) {
				e.printStackTrace();
			} catch (final NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (final CertificateException e) {
				e.printStackTrace();
			} catch (final IOException e) {
				e.printStackTrace();
			}
		} catch (final IOException e1) {
			logger.log(Level.WARNING, "Exception saving key store", e1);
		}
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
		} catch (@SuppressWarnings("unused") final Exception e) {
			source = new StringReader(keyFileLocation);
		}

		try (final PEMParser reader = new PEMParser(new BufferedReader(source))) {
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
		} catch (@SuppressWarnings("unused") final Exception e) {
			source = new StringReader(certFileLocation);
		}

		try (final PEMParser reader = new PEMParser(new BufferedReader(source))) {
			Object obj;

			final ArrayList<X509Certificate> chain = new ArrayList<>();

			while ((obj = reader.readObject()) != null)
				if (obj instanceof X509Certificate) {
					try {
						((X509Certificate) obj).checkValidity();
					} catch (final CertificateException e) {
						logger.log(Level.SEVERE, "Your certificate has expired or is invalid!", e);
						System.err.println("Your certificate has expired or is invalid:\n  " + e.getMessage());
						reader.close();
						return null;
					}
					chain.add((X509Certificate) obj);
				}
				else
					if (obj instanceof X509CertificateHolder) {
						final X509CertificateHolder ch = (X509CertificateHolder) obj;

						try {
							final X509Certificate c = new JcaX509CertificateConverter().setProvider("BC").getCertificate(ch);

							if (checkValidity)
								c.checkValidity();

							chain.add(c);
						} catch (final CertificateException ce) {
							logger.log(Level.SEVERE, "Exception loading certificate", ce);
						}
					}
					else
						System.err.println("Unknown object type: " + obj + "\n" + obj.getClass().getCanonicalName());

			if (chain.size() > 0)
				return chain.toArray(new X509Certificate[0]);

			return null;
		} catch (final IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	private static class JPasswordFinder {

		private final char[] password;

		JPasswordFinder(final char[] password) {
			this.password = password;
		}

		public char[] getPassword() {
			return Arrays.copyOf(password, password.length);
		}
	}

	private static final String charString = "!0123456789abcdefghijklmnopqrstuvwxyz@#$%^&*()-+=_{}[]:;|?/>.,<";

	/**
	 * @return randomized char array of passLength length
	 */
	public static char[] getRandomString() {
		final Random ran = new Random(System.nanoTime());
		final StringBuffer s = new StringBuffer();
		for (int i = 0; i < passLength; i++) {
			final int pos = ran.nextInt(charString.length());

			s.append(charString.charAt(pos));
		}
		return s.toString().toCharArray();
	}

	private static boolean keystore_loaded = false;

	/**
	 * @return true if JAliEn managed to load one of keystores
	 */
	public static boolean loadKeyStore() {
		keystore_loaded = false;

		// If JALIEN_TOKEN_CERT env var is set, token is in highest priority
		if (System.getenv("JALIEN_TOKEN_CERT") != null || tokenCertString != null)
			try {
				logger.log(Level.SEVERE, "Trying to load TOKEN CERT");
				if (loadTokenKeyStorage()) {
					logger.log(Level.SEVERE, "Loaded TOKEN CERT");
					keystore_loaded = true;
					return true;
				}
			} catch (final Exception e) {
				logger.log(Level.SEVERE, "Error loading token", e);
				System.err.println("Error loading token");
			}

		// Try to load full user certificate
		while (true)
			try {
				logger.log(Level.SEVERE, "Trying to load USER CERT");
				if (loadClientKeyStorage()) {
					logger.log(Level.SEVERE, "Loaded USER CERT");
					keystore_loaded = true;
					return true;
				}
				break;
			} catch (final org.bouncycastle.openssl.EncryptionException | org.bouncycastle.pkcs.PKCSException | javax.crypto.BadPaddingException e) {
				logger.log(Level.SEVERE, "Wrong password! Try again", e);
				System.err.println("Wrong password! Try again");
				continue;
			} catch (final Exception e) {
				logger.log(Level.SEVERE, "Error loading the key", e);
				System.err.println("Error loading the key");
				break;
			}

		// Try to load host cert
		try {
			logger.log(Level.SEVERE, "Trying to load HOST CERT");
			if (loadServerKeyStorage()) {
				logger.log(Level.SEVERE, "Loaded HOST CERT");
				keystore_loaded = true;
				return true;
			}
		} catch (final Exception e) {
			logger.log(Level.SEVERE, "Error loading hostcert", e);
			System.err.println("Error loading hostcert");
		}

		// Try to load token cert
		try {
			logger.log(Level.SEVERE, "Trying to load TOKEN CERT");
			if (loadTokenKeyStorage()) {
				logger.log(Level.SEVERE, "Loaded TOKEN CERT");
				keystore_loaded = true;
				return true;
			}
		} catch (final Exception e) {
			logger.log(Level.SEVERE, "Error loading token", e);
			System.err.println("Error loading token");
		}

		keystore_loaded = false;
		return false;
	}

	/**
	 * @return either tokenCert, clientCert or hostCert keystore
	 */
	public static KeyStore getKeyStore() {
		if (!keystore_loaded)
			loadKeyStore();

		if (System.getenv("JALIEN_TOKEN_CERT") != null || tokenCertString != null) {
			if (JAKeyStore.tokenCert != null) {
				return JAKeyStore.tokenCert;
			}
		}

		if (JAKeyStore.clientCert != null) {
			try {
				if (clientCert.getCertificateChain("User.cert") == null) {
					loadKeyStore();
				}
			} catch (final KeyStoreException e) {
				logger.log(Level.SEVERE, "Exception during loading client cert");
				e.printStackTrace();
			}
			return JAKeyStore.clientCert;
		}
		else
			if (JAKeyStore.hostCert != null) {
				try {
					if (hostCert.getCertificateChain("User.cert") == null)
						loadKeyStore();
				} catch (final KeyStoreException e) {
					logger.log(Level.SEVERE, "Exception during loading host cert");
					e.printStackTrace();
				}
				return JAKeyStore.hostCert;
			}
			else
				if (JAKeyStore.tokenCert != null) {
					try {
						if (tokenCert.getCertificateChain("User.cert") == null)
							loadKeyStore();
					} catch (final KeyStoreException e) {
						logger.log(Level.SEVERE, "Exception during loading token cert");
						e.printStackTrace();
					}
					return JAKeyStore.tokenCert;
				}

		return null;
	}
}
