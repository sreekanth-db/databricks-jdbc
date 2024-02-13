// This file is a copy of the file from the sqllogictest project (net/hydromatic/sqllogictest/Main.java)
package com.databricks.jdbc.sqllogictest;

import java.io.IOException;
import java.util.Set;
import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.TestLoader;
import net.hydromatic.sqllogictest.TestStatistics;
import net.hydromatic.sqllogictest.executors.NoExecutor;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

/** Execute all SqlLogicTest tests. */
public class SLTMain {
  private SLTMain() {}

  /** Command-line entry point. */
  public static void main(String[] args) throws IOException {
    OptionsParser optionParser = new OptionsParser(true, System.out, System.err);
    execute(optionParser, args);
  }

  /** Get the list of all test files. */
  public static Set<String> getTestList() {
    return new Reflections("test", Scanners.Resources).getResources(".*\\.test");
  }

  /**
   * Execute the program using the specified command-line options.
   *
   * @param optionParser Parser that will be used to parse the command-line options.
   * @param args Command-line options.
   * @return A description of the outcome of the tests. null when tests cannot even be started.
   */
  public static TestStatistics execute(OptionsParser optionParser, String... args)
      throws IOException {
    optionParser.setBinaryName("slt");
    NoExecutor.register(optionParser);
    DbsqlExecutor.register(optionParser);
    OptionsParser.SuppliedOptions options = optionParser.parse(args);
    if (options.exitCode != 0) {
      return null;
    }

    Set<String> allTests = getTestList();
    TestLoader loader = new TestLoader(options);
    for (String testPath : allTests) {
      boolean runTest = options.getDirectories().stream().anyMatch(testPath::contains);
      if (!runTest) {
        continue;
      }
      if (!loader.visitFile(testPath)) {
        break;
      }
    }
    return loader.statistics;
  }
}

// End Main.java
