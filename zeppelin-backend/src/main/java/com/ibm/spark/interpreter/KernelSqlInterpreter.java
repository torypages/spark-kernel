/*
 * Copyright 2015 IBM Corp.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.spark.interpreter;

import org.apache.spark.sql.DataFrame;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import java.util.List;

/**
 * Represents an interface from Zeppelin to the Spark Kernel, exposing SQL
 * in the same manner as the standard Zeppelin interface.
 *
 * <p>
 * NOTE: Currently, must run in a separate group from Zeppelin's "spark" due
 *       to the lack of ability to control/share the REPL class server.
 * </p>
 */
public class KernelSqlInterpreter extends Interpreter {
    private final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(this.getClass());

    private static final int MAX_RESULTS = 1000;
    private final ProgressRunner progressRunner = new ProgressRunner();
    private KernelInterpreter kernelInterpreter;
    private org.apache.spark.sql.SQLContext sqlContext;


    // Register our interpreter for Zeppelin to see
    static {
        Interpreter.register(
                "sparkkernelsql",
                "kernel",
                KernelSqlInterpreter.class.getName()
        );
    }

    public KernelSqlInterpreter(java.util.Properties properties) {
        super(properties);
    }

    @Override
    public void open() {
        progressRunner.clearProgress();

        this.kernelInterpreter = Utilities.findInterpreter(
                KernelInterpreter.class,
                getInterpreterGroup()
        );

        progressRunner.setProgress(25);

        this.sqlContext = this.kernelInterpreter.getSqlContext();

        progressRunner.setProgress(50);
    }

    @Override
    public void close() {
        assert kernelInterpreter != null :
                "Zeppelin Spark Kernel interpreter not created!";
        assert sqlContext != null : "SQL Context not created!";

        System.out.println("CLOSE!!!");
        progressRunner.stop();

        sqlContext = null;
        kernelInterpreter = null;
    }

    @Override
    public InterpreterResult interpret(final String s, final InterpreterContext interpreterContext) {
        assert sqlContext != null : "Spark SQL interpreter not started!";

        // Reset progress if not originating from an "open()"
        progressRunner.clearIfMax();

        // Mark progress to begin being incremented and then interpret our code
        progressRunner.turnOnIncrement();
        try {
            System.out.println("SQL: " + s);
            System.out.println("SQLContext: " + this.sqlContext);
            final DataFrame results = this.sqlContext.sql(s);
            System.out.println("Done SQL!");
            final String output = org.apache.zeppelin.spark.ZeppelinContext.showRDD(
                    sqlContext.sparkContext(),
                    interpreterContext,
                    results,
                    MAX_RESULTS
            );
            System.out.println("Showing: " + output);
            return new InterpreterResult(
                    org.apache.zeppelin.interpreter.InterpreterResult.Code.SUCCESS,
                    output
            );
        } catch (Exception ex) {
            final StringBuilder stackTraceBuilder = new StringBuilder();
            for (StackTraceElement ste : ex.getStackTrace()) {
                stackTraceBuilder.append(ste.toString() + "\n");
            }

            final String message =
                    "Name: " + ex.getClass().getName() + "\n" +
                    "Message: " + ex.getLocalizedMessage() + "\n" +
                    "Stack Trace: " + stackTraceBuilder.toString();
            return new InterpreterResult(
                    org.apache.zeppelin.interpreter.InterpreterResult.Code.ERROR,
                    message
            );
        } finally {
            progressRunner.turnOffIncrement();
        }

    }

    @Override
    public void cancel(InterpreterContext interpreterContext) {
        // TODO: Implement cancellation via Spark
    }

    @Override
    public FormType getFormType() {
        return FormType.SIMPLE;
    }

    @Override
    public int getProgress(InterpreterContext interpreterContext) {
        return progressRunner.getProgress();
    }

    @Override
    public List<String> completion(String s, int i) {
        // TODO: Add SQL completion
        return null;
    }
}
