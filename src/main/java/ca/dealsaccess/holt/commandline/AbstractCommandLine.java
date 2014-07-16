/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.dealsaccess.holt.commandline;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import ca.dealsaccess.holt.commandline.CommandLineUtil;
import ca.dealsaccess.holt.commandline.DefaultOptionCreator;

import com.google.common.collect.Lists;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public abstract class AbstractCommandLine {

	private static final Logger log = LoggerFactory.getLogger(AbstractCommandLine.class);

	/** input path, populated by {@link #parseArguments(String[])} */
	protected String inputPath;
	protected File inputFile; // the input represented as a file

	/** output path, populated by {@link #parseArguments(String[])} */
	protected String outputPath;
	protected File outputFile; // the output represented as a file

	/** temp path, populated by {@link #parseArguments(String[])} */
	protected String tempPath;

	protected Map<String, List<String>> argMap;

	/** internal list of options that have been added */
	private final List<Option> options;
	private Group group;

	protected AbstractCommandLine() {
		options = Lists.newLinkedList();
	}
	
	/**
	 * Returns the input path established by a call to
	 * {@link #parseArguments(String[])}. The source of the path may be an input
	 * option added using {@link #addInputOption()}
	 */
	protected String getInputPath() {
		return inputPath;
	}

	/**
	 * Returns the output path established by a call to
	 * {@link #parseArguments(String[])}. The source of the path may be an
	 * output option added using {@link #addOutputOption()}
	 */
	protected String getOutputPath() {
		return outputPath;
	}

	protected String getOutputPath(String path) {
		return new File(outputPath, path).getAbsolutePath();
	}

	protected File getInputFile() {
		return inputFile;
	}

	protected File getOutputFile() {
		return outputFile;
	}

	protected String getTempPath() {
		return tempPath;
	}

	protected String getTempPath(String directory) {
		return new File(tempPath, directory).getAbsolutePath();
	}

	/**
	 * Add an option with no argument whose presence can be checked for using
	 * {@code containsKey} method on the map returned by
	 * {@link #parseArguments(String[])};
	 */
	protected void addFlag(String name, String shortName, String description) {
		options.add(buildOption(name, shortName, description, false, false, null));
	}

	/**
	 * Add an option to the the set of options this job will parse when
	 * {@link #parseArguments(String[])} is called. This options has an argument
	 * with null as its default value.
	 */
	protected void addOption(String name, String shortName, String description) {
		options.add(buildOption(name, shortName, description, true, false, null));
	}

	/**
	 * Add an option to the the set of options this job will parse when
	 * {@link #parseArguments(String[])} is called.
	 * 
	 * @param required
	 *            if true the {@link #parseArguments(String[])} will throw fail
	 *            with an error and usage message if this option is not
	 *            specified on the command line.
	 */
	protected void addOption(String name, String shortName, String description, boolean required) {
		options.add(buildOption(name, shortName, description, true, required, null));
	}

	/**
	 * Add an option to the the set of options this job will parse when
	 * {@link #parseArguments(String[])} is called. If this option is not
	 * specified on the command line the default value will be used.
	 * 
	 * @param defaultValue
	 *            the default argument value if this argument is not found on
	 *            the command-line. null is allowed.
	 */
	protected void addOption(String name, String shortName, String description, String defaultValue) {
		options.add(buildOption(name, shortName, description, true, false, defaultValue));
	}

	/**
	 * Add an arbitrary option to the set of options this job will parse when
	 * {@link #parseArguments(String[])} is called. If this option has no
	 * argument, use {@code containsKey} on the map returned by
	 * {@code parseArguments} to check for its presence. Otherwise, the string
	 * value of the option will be placed in the map using a key equal to this
	 * options long name preceded by '--'.
	 * 
	 * @return the option added.
	 */
	protected Option addOption(Option option) {
		options.add(option);
		return option;
	}

	protected Group getGroup() {
		return group;
	}

	/**
	 * Add the default input directory option, '-i' which takes a directory name
	 * as an argument. When {@link #parseArguments(String[])} is called, the
	 * inputPath will be set based upon the value for this option. If this
	 * method is called, the input is required.
	 */
	protected void addInputOption() {
		addOption(DefaultOptionCreator.inputOption().create());
	}

	/**
	 * Add the default output directory option, '-o' which takes a directory
	 * name as an argument. When {@link #parseArguments(String[])} is called,
	 * the outputPath will be set based upon the value for this option. If this
	 * method is called, the output is required.
	 */
	protected void addOutputOption() {
		addOption(DefaultOptionCreator.outputOption().create());
	}

	/**
	 * Build an option with the given parameters. Name and description are
	 * required.
	 * 
	 * @param name
	 *            the long name of the option prefixed with '--' on the
	 *            command-line
	 * @param shortName
	 *            the short name of the option, prefixed with '-' on the
	 *            command-line
	 * @param description
	 *            description of the option displayed in help method
	 * @param hasArg
	 *            true if the option has an argument.
	 * @param required
	 *            true if the option is required.
	 * @param defaultValue
	 *            default argument value, can be null.
	 * @return the option.
	 */
	protected static Option buildOption(String name, String shortName, String description, boolean hasArg,
			boolean required, String defaultValue) {

		return buildOption(name, shortName, description, hasArg, 1, 1, required, defaultValue);
	}

	protected static Option buildOption(String name, String shortName, String description, boolean hasArg, int min,
			int max, boolean required, String defaultValue) {

		DefaultOptionBuilder optBuilder = new DefaultOptionBuilder().withLongName(name).withDescription(description)
				.withRequired(required);

		if (shortName != null) {
			optBuilder.withShortName(shortName);
		}

		if (hasArg) {
			ArgumentBuilder argBuilder = new ArgumentBuilder().withName(name).withMinimum(min).withMaximum(max);

			if (defaultValue != null) {
				argBuilder = argBuilder.withDefault(defaultValue);
			}

			optBuilder.withArgument(argBuilder.create());
		}

		return optBuilder.create();
	}

	/**
	 * @param name
	 *            The name of the option
	 * @return the {@link org.apache.commons.cli2.Option} with the name, else
	 *         null
	 */
	protected Option getCLIOption(String name) {
		for (Option option : options) {
			if (option.getPreferredName().equals(name)) {
				return option;
			}
		}
		return null;
	}

	/**
	 * Parse the arguments specified based on the options defined using the
	 * various {@code addOption} methods. If -h is specified or an exception is
	 * encountered print help and return null. Has the side effect of setting
	 * inputPath and outputPath if {@code addInputOption} or
	 * {@code addOutputOption} or {@code mapred.input.dir} or
	 * {@code mapred.output.dir} are present in the Configuration.
	 * 
	 * @return a {@code Map<String,String>} containing options and their
	 *         argument values. The presence of a flag can be tested using
	 *         {@code containsKey}, while argument values can be retrieved using
	 *         {@code get(optionName)}. The names used for keys are the option
	 *         name parameter prefixed by '--'.
	 * 
	 * @see #parseArguments(String[], boolean, boolean) -- passes in false,
	 *      false for the optional args.
	 */
	public Map<String, List<String>> parseArguments(String[] args) throws IOException {
		return parseArguments(args, false, false);
	}

	/**
	 * 
	 * @param args
	 *            The args to parse
	 * @param inputOptional
	 *            if false, then the input option, if set, need not be present.
	 *            If true and input is an option and there is no input, then
	 *            throw an error
	 * @param outputOptional
	 *            if false, then the output option, if set, need not be present.
	 *            If true and output is an option and there is no output, then
	 *            throw an error
	 * @return the args parsed into a map.
	 */
	public Map<String, List<String>> parseArguments(String[] args, boolean inputOptional, boolean outputOptional)
			throws IOException {
		Option helpOpt = addOption(DefaultOptionCreator.helpOption());
		addOption("tempDir", null, "Intermediate output directory", "temp");

		GroupBuilder gBuilder = new GroupBuilder().withName("Job-Specific Options:");

		for (Option opt : options) {
			gBuilder = gBuilder.withOption(opt);
		}

		group = gBuilder.create();

		CommandLine cmdLine;
		try {
			Parser parser = new Parser();
			parser.setGroup(group);
			parser.setHelpOption(helpOpt);
			cmdLine = parser.parse(args);

		} catch (OptionException e) {
			log.error(e.getMessage());
			CommandLineUtil.printHelpWithGenericOptions(group, e);
			return null;
		}

		if (cmdLine.hasOption(helpOpt)) {
			CommandLineUtil.printHelpWithGenericOptions(group);
			return null;
		}

		argMap = new TreeMap<String, List<String>>();
		maybePut(argMap, cmdLine, this.options.toArray(new Option[this.options.size()]));

		this.tempPath = getOption("tempDir");

		if (!hasOption("quiet")) {
			log.info("Command line arguments: {}", argMap);
		}
		return argMap;
	}

	/**
	 * Build the option key (--name) from the option name
	 */
	public static String keyFor(String optionName) {
		return "--" + optionName;
	}

	/**
	 * @return the requested option, or null if it has not been specified
	 */
	public String getOption(String optionName) {
		List<String> list = argMap.get(keyFor(optionName));
		if (list != null && !list.isEmpty()) {
			return list.get(0);
		}
		return null;
	}

	/**
	 * Get the option, else the default
	 * 
	 * @param optionName
	 *            The name of the option to look up, without the --
	 * @param defaultVal
	 *            The default value.
	 * @return The requested option, else the default value if it doesn't exist
	 */
	public String getOption(String optionName, String defaultVal) {
		String res = getOption(optionName);
		if (res == null) {
			res = defaultVal;
		}
		return res;
	}

	public int getInt(String optionName) {
		return Integer.parseInt(getOption(optionName));
	}

	public int getInt(String optionName, int defaultVal) {
		return Integer.parseInt(getOption(optionName, String.valueOf(defaultVal)));
	}

	public float getFloat(String optionName) {
		return Float.parseFloat(getOption(optionName));
	}

	public float getFloat(String optionName, float defaultVal) {
		return Float.parseFloat(getOption(optionName, String.valueOf(defaultVal)));
	}

	/**
	 * Options can occur multiple times, so return the list
	 * 
	 * @param optionName
	 *            The unadorned (no "--" prefixing it) option name
	 * @return The values, else null. If the option is present, but has no
	 *         values, then the result will be an empty list
	 *         (Collections.emptyList())
	 */
	public List<String> getOptions(String optionName) {
		return argMap.get(keyFor(optionName));
	}

	/**
	 * @return if the requested option has been specified
	 */
	public boolean hasOption(String optionName) {
		return argMap.containsKey(keyFor(optionName));
	}

	
	

	protected static void maybePut(Map<String, List<String>> args, CommandLine cmdLine, Option... opt) {
		for (Option o : opt) {

			// the option appeared on the command-line, or it has a value
			// (which is likely a default value).
			if (cmdLine.hasOption(o) || cmdLine.getValue(o) != null
					|| (cmdLine.getValues(o) != null && !cmdLine.getValues(o).isEmpty())) {

				// nulls are ok, for cases where options are simple flags.
				List<?> vo = cmdLine.getValues(o);
				if (vo != null && !vo.isEmpty()) {
					List<String> vals = Lists.newArrayList();
					for (Object o1 : vo) {
						vals.add(o1.toString());
					}
					args.put(o.getPreferredName(), vals);
				} else {
					args.put(o.getPreferredName(), null);
				}
			}
		}
	}

	/**
	 * 
	 * @param args
	 *            The input argument map
	 * @param optName
	 *            The adorned (including "--") option name
	 * @return The first value in the match, else null
	 */
	public static String getOption(Map<String, List<String>> args, String optName) {
		List<String> res = args.get(optName);
		if (res != null && !res.isEmpty()) {
			return res.get(0);
		}
		return null;
	}
}
