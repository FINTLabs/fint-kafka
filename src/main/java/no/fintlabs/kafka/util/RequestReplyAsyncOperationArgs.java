package no.fintlabs.kafka.util;

import java.util.function.Consumer;

public class RequestReplyAsyncOperationArgs<V, R> extends RequestReplyOperationArgs<V, R> {

    public final Consumer<R> successCallback;
    public final Consumer<Throwable> failureCallback;

    public RequestReplyAsyncOperationArgs(RequestReplyOperationArgs<V, R> requestReplyOperationArgs, Consumer<R> successCallback, Consumer<Throwable> failureCallback) {
        super(requestReplyOperationArgs);
        this.successCallback = successCallback;
        this.failureCallback = failureCallback;
    }

}
