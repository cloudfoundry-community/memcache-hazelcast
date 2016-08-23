package cloudfoundry.memcache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CompletedFuture implements Future<Void> {
	
	public static final Future<Void> INSTANCE = new CompletedFuture();

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public Void get() throws InterruptedException, ExecutionException {
		return null;
	}
	
	@Override
	public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return null;
	}
	
	@Override
	public boolean isCancelled() {
		return false;
	}
	
	@Override
	public boolean isDone() {
		return true;
	}
	
	
}
