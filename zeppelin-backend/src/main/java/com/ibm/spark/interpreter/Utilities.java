package com.ibm.spark.interpreter;

import org.apache.zeppelin.interpreter.InterpreterGroup;

/**
 * Represents extra utility functions needed by the Zeppelin interpreters.
 */
public class Utilities {

    /**
     * Retrieves the interpreter matching the specified class from the
     * provided interpreter group, opening any lazy interpreters along the
     * way.
     *
     * @param klass The class of the interpreter to retrieve
     * @param interpreterGroup The group of interpreters to search through
     * @param <T> The type of interpreter
     *
     * @return The interpreter instance if found, otherwise null
     */
    static <T extends org.apache.zeppelin.interpreter.Interpreter>
    T findInterpreter(Class<T> klass, InterpreterGroup interpreterGroup) {
        for (org.apache.zeppelin.interpreter.Interpreter i : interpreterGroup) {
            if (klass.getName().equals(i.getClassName())) {
                org.apache.zeppelin.interpreter.Interpreter interpreter = i;

                while (interpreter instanceof org.apache.zeppelin.interpreter.WrappedInterpreter) {
                    // Open the interpreter if it has not been already
                    if (interpreter instanceof org.apache.zeppelin.interpreter.LazyOpenInterpreter) {
                        interpreter.open();
                    }

                    // Follow the nested series of interpreters
                    interpreter = ((org.apache.zeppelin.interpreter.WrappedInterpreter) interpreter).getInnerInterpreter();
                }

                return klass.cast(interpreter);
            }
        }

        return null;
    }

}
