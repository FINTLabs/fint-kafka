package no.fintlabs.util;

public class AsyncGetHandler {

    public final Object syncObject;

    private final int requiredNumberOfSuccesses;
    private int numberOfSuccesses;

    public AsyncGetHandler(int requiredNumberOfSuccesses) {
        this.requiredNumberOfSuccesses = requiredNumberOfSuccesses;
        this.syncObject = new Object();
        this.numberOfSuccesses = 0;
    }

    public void successCallback() {
        this.numberOfSuccesses++;
        if (this.numberOfSuccesses == this.requiredNumberOfSuccesses) {
            synchronized (syncObject) {
                syncObject.notify();
            }
        }
    }
}
