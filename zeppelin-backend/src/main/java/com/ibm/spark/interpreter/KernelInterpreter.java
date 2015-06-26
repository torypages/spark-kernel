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

import com.ibm.spark.boot.CommandLineOptions;
import com.ibm.spark.boot.KernelBootstrap$;
import com.typesafe.config.Config;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.Interpreter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an interface from Zeppelin to the Spark Kernel.
 *
 * <p>
 * NOTE: Currently, must run in a separate group from Zeppelin's "spark" due
 *       to the lack of ability to control/share the REPL class server.
 * </p>
 */
public class KernelInterpreter extends Interpreter {
    private final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(this.getClass());

    private com.ibm.spark.boot.KernelBootstrap kernelBootstrap;
    public com.ibm.spark.boot.KernelBootstrap getKernelBootstrap() {
        return kernelBootstrap;
    }

    private com.ibm.spark.kernel.api.KernelLike kernel;
    public com.ibm.spark.kernel.api.KernelLike getKernel() {
        return kernel;
    }

    private org.apache.spark.SparkContext sparkContext;
    public org.apache.spark.SparkContext getSparkContext() {
        return sparkContext;
    }

    private org.apache.spark.sql.SQLContext sqlContext;
    public org.apache.spark.sql.SQLContext getSqlContext() {
        return sqlContext;
    }

    private com.ibm.spark.interpreter.Interpreter interpreter;
    public com.ibm.spark.interpreter.Interpreter getInterpreter() {
        return interpreter;
    }

    private com.ibm.spark.magic.MagicLoader magicLoader;
    public com.ibm.spark.magic.MagicLoader getMagicLoader() {
        return magicLoader;
    }

    private final ProgressRunner progressRunner = new ProgressRunner();

    private final List<String> arguments = new ArrayList<String>();

    // Register our interpreter for Zeppelin to see
    static {
        Interpreter.register(
                "sparkkernel",
                "kernel",
                KernelInterpreter.class.getName()
        );
    }

    public KernelInterpreter(java.util.Properties properties) {
        super(properties);

        // Build up our argument list
        for (String propertyName : properties.stringPropertyNames()) {
            final String propertyValue =
                    properties.getProperty(propertyName, "");
            final String argumentString =
                    "-S" + propertyName + "=" + propertyValue;
            final String p = "(" + propertyName + ":" + propertyValue + ")";

            logger.info("Using " + p + " as " + argumentString);
            arguments.add(argumentString);
        }
    }

    @Override
    public void open() {
        progressRunner.clearProgress();

        final Config config = new CommandLineOptions(
                scala.collection.JavaConverters.asScalaBufferConverter(arguments).asScala().toList()
        ).toConfig();

        // Stand up a Spark Kernel without the Spark Context
        kernelBootstrap = KernelBootstrap$.MODULE$.standardKernelBootstrap(
                config
        ).initialize();

        progressRunner.setProgress(25);

        // Get the collection of interpreters
        final java.util.List<com.ibm.spark.interpreter.Interpreter> interpreters =
                scala.collection.JavaConverters.asJavaListConverter(kernelBootstrap.getInterpreters()).asJava();

        // Retrieve the kernel instance created by the bootstrapping
        kernel = kernelBootstrap.getKernel();

        // Retrieve the Spark Context created by the bootstrapping
        sparkContext = kernelBootstrap.getSparkContext();

        // Retrieve the SQL Context created by the bootstrapping
        sqlContext = kernelBootstrap.getSqlContext();

        // Use the first interpreter to represent this Zeppelin interface
        interpreter = interpreters.get(0);

        // Retrieve the magic loader used by the kernel
        magicLoader = kernelBootstrap.getMagicLoader();

        // TODO: This is a hack since we are getting null for our output stream
        // NOTE: The output is not being picked up by Zeppelin frontend
        magicLoader.dependencyMap().setOutputStream(System.out);
    }

    @Override
    public void close() {
        assert magicLoader != null : "Spark Kernel magic loader not created!";
        assert interpreter != null : "Spark Kernel interpreter not started!";
        assert kernel != null : "Spark Kernel API not created!";
        assert kernelBootstrap != null : "Spark Kernel not bootstrapped!";

        progressRunner.stop();
        interpreter.stop();

        magicLoader = null;
        interpreter = null;
        kernel = null;
        kernelBootstrap = null;
    }

    @Override
    public InterpreterResult interpret(final String s, final InterpreterContext interpreterContext) {
        assert interpreter != null : "Spark Kernel interpreter not started!";

        // Reset progress if not originating from an "open()"
        progressRunner.clearIfMax();

        // Mark progress to begin being incremented and then interpret our code
        progressRunner.turnOnIncrement();
        final scala.util.Either<String, com.ibm.spark.interpreter.ExecuteFailure> results =
                interpreter.interpret(s, false)._2();

        progressRunner.turnOffIncrement();
        progressRunner.maximizeProgress();
        if (results.isLeft()) {
            return new InterpreterResult(InterpreterResult.Code.SUCCESS, results.left().get());
        } else {
            return new InterpreterResult(InterpreterResult.Code.ERROR, results.right().get().toString());
        }
    }

    @Override
    public void cancel(InterpreterContext interpreterContext) {
        assert interpreter != null : "Spark Kernel interpreter not started!";

        interpreter.interrupt();
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
        assert interpreter != null : "Spark Kernel interpreter not started!";

        final scala.collection.Seq<String> results =
                interpreter.completion(s, i)._2();
        return scala.collection.JavaConverters.asJavaListConverter(results).asJava();
    }
}
