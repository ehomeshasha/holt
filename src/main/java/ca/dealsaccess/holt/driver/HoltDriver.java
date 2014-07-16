package ca.dealsaccess.holt.driver;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HoltDriver {

	private static final Logger log = LoggerFactory.getLogger(HoltDriver.class);

	private HoltDriver() {
	}

	public static void main(String[] args) throws Throwable {

		Properties mainClasses = loadProperties("driver.classes.props");
		if (mainClasses == null) {
			mainClasses = loadProperties("driver.classes.default.props");
		}
		if (mainClasses == null) {
			throw new IOException("Can't load any properties file?");
		}

		boolean foundShortName = false;
		ProgramDriver programDriver = new ProgramDriver();
		for (Object key : mainClasses.keySet()) {
			String keyString = (String) key;
			if (args.length > 0 && shortName(mainClasses.getProperty(keyString)).equals(args[0])) {
				foundShortName = true;
			}
			if (args.length > 0 && keyString.equalsIgnoreCase(args[0]) && isDeprecated(mainClasses, keyString)) {
				log.error(desc(mainClasses.getProperty(keyString)));
				return;
			}
			if (isDeprecated(mainClasses, keyString)) {
				continue;
			}
			addClass(programDriver, keyString, mainClasses.getProperty(keyString));
		}

		if (args.length < 1 || args[0] == null || "-h".equals(args[0]) || "--help".equals(args[0])) {
			programDriver.driver(args);
			return;
		}

		String progName = args[0];
		if (!foundShortName) {
			addClass(programDriver, progName, progName);
		}
		shift(args);

		Properties mainProps = loadProperties(progName + ".props");
		if (mainProps == null) {
			log.warn("No {}.props found on classpath, will use command-line arguments only", progName);
			mainProps = new Properties();
		}

		Map<String, String[]> argMap = Maps.newHashMap();
		int i = 0;
		while (i < args.length && args[i] != null) {
			List<String> argValues = Lists.newArrayList();
			String arg = args[i];
			i++;
			if (arg.startsWith("-D")) { // '-Dkey=value' or
										// '-Dkey=value1,value2,etc' case
				String[] argSplit = arg.split("=");
				arg = argSplit[0];
				if (argSplit.length == 2) {
					argValues.add(argSplit[1]);
				}
			} else { // '-key [values]' or '--key [values]' case.
				while (i < args.length && args[i] != null) {
					if (args[i].startsWith("-")) {
						break;
					}
					argValues.add(args[i]);
					i++;
				}
			}
			argMap.put(arg, argValues.toArray(new String[argValues.size()]));
		}

		// Add properties from the .props file that are not overridden on the
		// command line
		for (String key : mainProps.stringPropertyNames()) {
			String[] argNamePair = key.split("\\|");
			String shortArg = '-' + argNamePair[0].trim();
			String longArg = argNamePair.length < 2 ? null : "--" + argNamePair[1].trim();
			if (!argMap.containsKey(shortArg) && (longArg == null || !argMap.containsKey(longArg))) {
				argMap.put(longArg, new String[] { mainProps.getProperty(key) });
			}
		}

		// Now add command-line args
		List<String> argsList = Lists.newArrayList();
		argsList.add(progName);
		for (Map.Entry<String, String[]> entry : argMap.entrySet()) {
			String arg = entry.getKey();
			if (arg.startsWith("-D")) { // arg is -Dkey - if value for this
										// !isEmpty(), then arg -> -Dkey + "=" +
										// value
				String[] argValues = entry.getValue();
				if (argValues.length > 0 && !argValues[0].trim().isEmpty()) {
					arg += '=' + argValues[0].trim();
				}
				argsList.add(1, arg);
			} else {
				argsList.add(arg);
				for (String argValue : Arrays.asList(argMap.get(arg))) {
					if (!argValue.isEmpty()) {
						argsList.add(argValue);
					}
				}
			}
		}

		long start = System.currentTimeMillis();

		programDriver.driver(argsList.toArray(new String[argsList.size()]));

		if (log.isInfoEnabled()) {
			log.info("Program took {} ms (Minutes: {})", System.currentTimeMillis() - start,
					(System.currentTimeMillis() - start) / 60000.0);
		}
	}

	private static boolean isDeprecated(Properties mainClasses, String keyString) {
		return "deprecated".equalsIgnoreCase(shortName(mainClasses.getProperty(keyString)));
	}

	private static Properties loadProperties(String resource) throws IOException {
		InputStream propsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		if (propsStream != null) {
			try {
				Properties properties = new Properties();
				properties.load(propsStream);
				return properties;
			} finally {
				Closeables.close(propsStream, true);
			}
		}
		return null;
	}

	private static String[] shift(String[] args) {
		System.arraycopy(args, 1, args, 0, args.length - 1);
		args[args.length - 1] = null;
		return args;
	}

	private static String shortName(String valueString) {
		return valueString.contains(":") ? valueString.substring(0, valueString.indexOf(':')).trim() : valueString;
	}

	private static String desc(String valueString) {
		return valueString.contains(":") ? valueString.substring(valueString.indexOf(':')).trim() : valueString;
	}

	private static void addClass(ProgramDriver driver, String classString, String descString) {
		try {
			Class<?> clazz = Class.forName(classString);
			driver.addClass(shortName(descString), clazz, desc(descString));
		} catch (ClassNotFoundException e) {
			log.warn("Unable to add class: {}", classString, e);
		} catch (Throwable t) {
			log.warn("Unable to add class: {}", classString, t);
		}
	}

}
