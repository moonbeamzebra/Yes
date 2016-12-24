package ca.magenta.yes;

import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * @author moonbeam <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-12-03
 */
public class Yes {

/*
// To run:
java -jar \
-Dloader.main=ca.magenta.yes.Yes \
target/ca.magenta.yes-1.0-SNAPSHOT.jar \
-apiServerAddr=127.0.0.1 -apiServerPort=9595 10.10.10.30
*/

	private static String version = "0.1 (2016-12-23)";

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Yes.class.getPackage().getName());

	private static String searchString = null;
	private static String apiServerAddr_opt = null;
	private static int apiServerPort_opt = -1;

	public static final void main(String[] args) {

		int rc = 0;

		logger.info("");
		logger.info("Running Yes version " + version);

		rc = parseParam(args);

		if (rc == 0) {

			try {

				Socket apiServer = new Socket(apiServerAddr_opt, apiServerPort_opt);
				PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

				toServer.println(searchString);

				BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

				String inputLine;

				while ((inputLine = fromServer.readLine()) != null) {
					logger.info(inputLine);

				}

				fromServer.close();
				apiServer.close();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
	}

	private static int parseParam(String a_sArgs[]) {
		int rc = 0;

		if (a_sArgs.length > 0) {
			for (int i = 0; i < a_sArgs.length; i++) {
				if (a_sArgs[i].startsWith("-apiServerAddr=")) {
					apiServerAddr_opt = a_sArgs[i].substring(15);
					logger.info("apiServerAddr: [" + apiServerAddr_opt + "]");
				} else if (a_sArgs[i].startsWith("-apiServerPort=")) {
					String apiServerPortStr = a_sArgs[i].substring(15);
					try {
						apiServerPort_opt = Integer.parseInt(apiServerPortStr);
						logger.info("apiServerPort: [" + apiServerPort_opt + "]");

					} catch (NumberFormatException e) {
						logger.error("Bad apiServerPort: [" + apiServerPortStr + "]");
						rc = 1;
					}
				} else if (a_sArgs[i].startsWith("-")) {
					rc = 1;
				} else {
					String searchStringPart = a_sArgs[i];
					if (searchString == null)
						searchString = searchStringPart;
					else
						searchString = searchString + " " + searchStringPart;
				}
			}
		}


		if ((apiServerAddr_opt == null) ||
				(apiServerPort_opt == -1) ||
				(searchString == null) ||
				(rc != 0)) {
			System.err.println("Usage: Yes -apiServerAddr=apiServerAddr -apiServerPort=msgServerPort searchString");

			System.err.println("Ex:    Yes -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");

			rc = 1;
		}
		else
			logger.info("searchString: [" + searchString + "]");


		return rc;
	}

}
