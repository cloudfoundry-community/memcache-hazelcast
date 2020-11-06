package cloudfoundry.memcache;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownUtils {
	@FunctionalInterface
	public interface CheckedRunnable {
		void run() throws Exception;
	}

	private static final Logger LOG = LoggerFactory.getLogger(ShutdownUtils.class);

	private static volatile boolean shutdownStarted = false;

	private ShutdownUtils() {
	}

	public static synchronized void gracefullyExit(String msg, byte code, Throwable cause) {
		Throwable loggedCause;
		if (cause != null) {
			loggedCause = cause;
		} else {
			loggedCause = new Exception("No cause provided but this Exception provides the trace that got us here.");
		}
		synchronized (ShutdownUtils.class) {
			if (!shutdownStarted) {
				LOG.error("Shutting down app with code={} and msg={}", code, msg, loggedCause);
				new Thread(() -> System.exit(code)).start();
				shutdownStarted = true;
			}
		}
	}

	/**
	 * Executes the closeTask catching any exception thrown and logging a warning
	 * and a debug message on failure.
	 *
	 * @param closeTask
	 * @param log
	 * @param failureMsg
	 */
	public static void safeClose(CheckedRunnable closeTask, Logger log, String failureMsg) {
		try {
			closeTask.run();
		} catch (Exception e) {
			log.warn("{}.  See debug log for details.", failureMsg);
			log.debug(failureMsg, e);
		}
	}

	/**
	 * Gracefully closes the executor service and logs when we have to interrupt.
	 *
	 * @param executorService         the service that we are trying to close.
	 * @param awaitTerminationTimeOut the duration we check if the executor service
	 *                                is waiting for.
	 * @param giveupTimeout           the amount of time that is appropriate for us
	 *                                to wait for the service to close before we
	 *                                interrupt.
	 */
	public static void gracefullyCloseExecutorService(ExecutorService executorService, Duration awaitTerminationTimeOut,
			Duration giveupTimeout) {
		executorService.shutdown();
		try {
			if (!executorService.awaitTermination(awaitTerminationTimeOut.toMillis(), TimeUnit.MILLISECONDS)) {
				long giveUpTime = System.currentTimeMillis() + giveupTimeout.toMillis();
				LOG.warn("Await termination time expired.  Attempting to interrupt every 500ms for {}", giveupTimeout);
				do {
					executorService.shutdownNow();
				} while (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)
						&& giveUpTime > System.currentTimeMillis());
				if (!executorService.isTerminated()) {

					LOG.error("Failed to terminate all threads, gave up.");
				}
			}

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
