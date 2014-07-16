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

import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;

public final class DefaultOptionCreator {

	public static final String INPUT_OPTION = "input";

	public static final String OUTPUT_OPTION = "output";

	private DefaultOptionCreator() {}

	/**
	 * Returns a default command line option for help. Used by all clustering
	 * jobs and many others
	 * */
	public static Option helpOption() {
		return new DefaultOptionBuilder().withLongName("help").withDescription("Print out help").withShortName("h")
				.create();
	}

	/**
	 * Returns a default command line option for input directory specification.
	 * Used by all clustering jobs plus others
	 */
	public static DefaultOptionBuilder inputOption() {
		return new DefaultOptionBuilder().withLongName(INPUT_OPTION).withRequired(false).withShortName("i")
				.withArgument(new ArgumentBuilder().withName(INPUT_OPTION).withMinimum(1).withMaximum(1).create())
				.withDescription("Path to job input directory.");
	}

	/**
	 * Returns a default command line option for output directory specification.
	 * Used by all clustering jobs plus others
	 */
	public static DefaultOptionBuilder outputOption() {
		return new DefaultOptionBuilder().withLongName(OUTPUT_OPTION).withRequired(false).withShortName("o")
				.withArgument(new ArgumentBuilder().withName(OUTPUT_OPTION).withMinimum(1).withMaximum(1).create())
				.withDescription("The directory pathname for output.");
	}

}
