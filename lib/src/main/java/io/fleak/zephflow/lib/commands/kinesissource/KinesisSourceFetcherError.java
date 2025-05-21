package io.fleak.zephflow.lib.commands.kinesissource;

public class KinesisSourceFetcherError extends RuntimeException {
  public KinesisSourceFetcherError(String message) {
    super(message);
  }

  public KinesisSourceFetcherError(String message, Throwable cause) {
    super(message, cause);
  }
}
