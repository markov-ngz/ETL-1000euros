package com.markovngz.rabbitmq;

import java.util.function.Consumer;

/*
 * Extending Consumer interface in order to: 
 * allow an error to be raised in a lambda function
 * @source : https://stackoverflow.com/questions/18198176/java-8-lambda-function-that-throws-exception 
 * @credits : https://stackoverflow.com/users/222438/jlb 
 */
@FunctionalInterface
public interface ThrowingConsumer<T> extends Consumer<T> {

    @Override
    default void accept(final T elem) {
        try {
            acceptThrows(elem);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    void acceptThrows(T elem) throws Exception;

}