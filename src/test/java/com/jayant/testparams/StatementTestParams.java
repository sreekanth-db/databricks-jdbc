package com.jayant.testparams;

import static com.jayant.testparams.ParamUtils.putInMapForKey;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class StatementTestParams implements TestParams {

  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> set = new HashSet<>();

    // Do not close the shared statement
    set.add(Map.entry("close", 0));

    // Cancel needs an active query
    set.add(Map.entry("cancel", 0));

    // Void side effects on shared object
    set.add(Map.entry("clearWarnings", 0));
    set.add(Map.entry("closeOnCompletion", 0));
    set.add(Map.entry("clearBatch", 0));

    // Returns object references (not comparable across drivers)
    set.add(Map.entry("getConnection", 0));

    // Execution methods - tested via SQL query comparator tests
    set.add(Map.entry("executeQuery", 1));
    set.add(Map.entry("executeUpdate", 1));
    set.add(Map.entry("execute", 1));
    set.add(Map.entry("executeLargeUpdate", 1));
    set.add(Map.entry("executeUpdate", 2));
    set.add(Map.entry("execute", 2));
    set.add(Map.entry("executeLargeUpdate", 2));

    // Batch operations - DML side effects
    set.add(Map.entry("executeBatch", 0));
    set.add(Map.entry("executeLargeBatch", 0));
    set.add(Map.entry("addBatch", 1));

    // Not implemented / throws
    set.add(Map.entry("setCursorName", 1));

    // Driver-specific wrapper methods
    set.add(Map.entry("unwrap", 1));
    set.add(Map.entry("isWrapperFor", 1));

    return set;
  }

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();

    // SQL quoting methods
    putInMapForKey(
        functionToArgsMap, Map.entry("enquoteLiteral", 1), new Object[] {"test's value"});
    putInMapForKey(
        functionToArgsMap, Map.entry("enquoteIdentifier", 2), new Object[] {"my column", true});
    putInMapForKey(
        functionToArgsMap, Map.entry("enquoteIdentifier", 2), new Object[] {"simple", false});
    putInMapForKey(functionToArgsMap, Map.entry("isSimpleIdentifier", 1), new Object[] {"simple"});
    putInMapForKey(
        functionToArgsMap, Map.entry("isSimpleIdentifier", 1), new Object[] {"has space"});
    putInMapForKey(
        functionToArgsMap, Map.entry("enquoteNCharLiteral", 1), new Object[] {"test string"});

    // Setter methods (compare void returns + catch exception differences)
    putInMapForKey(functionToArgsMap, Map.entry("setMaxRows", 1), new Object[] {10});
    putInMapForKey(functionToArgsMap, Map.entry("setLargeMaxRows", 1), new Object[] {10L});
    putInMapForKey(functionToArgsMap, Map.entry("setMaxFieldSize", 1), new Object[] {100});
    putInMapForKey(functionToArgsMap, Map.entry("setQueryTimeout", 1), new Object[] {30});
    putInMapForKey(functionToArgsMap, Map.entry("setFetchSize", 1), new Object[] {100});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("setFetchDirection", 1),
        new Object[] {ResultSet.FETCH_FORWARD});
    putInMapForKey(functionToArgsMap, Map.entry("setPoolable", 1), new Object[] {false});
    putInMapForKey(functionToArgsMap, Map.entry("setEscapeProcessing", 1), new Object[] {true});
    putInMapForKey(functionToArgsMap, Map.entry("setEscapeProcessing", 1), new Object[] {false});

    // getMoreResults with flag
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getMoreResults", 1),
        new Object[] {Statement.CLOSE_CURRENT_RESULT});

    return functionToArgsMap;
  }
}
