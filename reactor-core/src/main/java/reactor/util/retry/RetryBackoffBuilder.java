/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A builder for a retry strategy based on exponential backoffs, with fine grained options.
 * Retry delays are randomized with a user-provided {@link #jitter(double)} factor between {@code 0.d} (no jitter)
 * and {@code 1.0} (default is {@code 0.5}).
 * Even with the jitter, the effective backoff delay cannot be less than {@link #minBackoff(Duration)}
 * nor more than {@link #maxBackoff(Duration)}. The delays and subsequent attempts are executed on the
 * provided backoff {@link #scheduler(Scheduler)}.
 * <p>
 * Only errors that match the {@link #throwablePredicate(Predicate)} are retried (by default all),
 * and the number of attempts can also limited with {@link #maxAttempts(long)}.
 * When the maximum attempt of retries is reached, a runtime exception is propagated downstream which
 * can be pinpointed with {@link reactor.core.Exceptions#isRetryExhausted(Throwable)}. The cause of
 * the last attempt's failure is attached as said {@link reactor.core.Exceptions#retryExhausted(long, Throwable) retryExhausted}
 * exception's cause.
 * <p>
 * Additionally, to help dealing with bursts of transient errors in a long-lived Flux as if each burst
 * had its own backoff, one can choose to set {@link #transientErrors(boolean)} to {@code true}.
 * The comparison to {@link #maxAttempts(long)} will then be done with the number of subsequent attempts
 * that failed without an {@link org.reactivestreams.Subscriber#onNext(Object) onNext} in between.
 * <p>
 * The {@link RetryBackoffBuilder} is copy-on-write and as such can be stored as a "template" and further configured
 * by different components without a risk of modifying the original configuration.
 *
 * @author Simon Baslé
 */
public final class RetryBackoffBuilder implements Retry {

	final Duration  minBackoff;
	final Duration  maxBackoff;
	final double    jitterFactor;
	final Scheduler backoffScheduler;

	final long                 maxAttempts;
	final Predicate<Throwable> throwablePredicate;
	final boolean              isTransientErrors;

	final Consumer<RetrySignal>                           doPreRetry;
	final Consumer<RetrySignal>                           doPostRetry;
	final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry;
	final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry;

	/**
	 * Copy constructor.
	 */
	RetryBackoffBuilder(long max,
			Predicate<? super Throwable> aThrowablePredicate,
			boolean isTransientErrors,
			Duration minBackoff, Duration maxBackoff, double jitterFactor,
			Scheduler backoffScheduler,
			Consumer<RetrySignal> doPreRetry,
			Consumer<RetrySignal> doPostRetry,
			BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry,
			BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry) {
		this.maxAttempts = max;
		this.throwablePredicate = aThrowablePredicate::test; //massaging type
		this.isTransientErrors = isTransientErrors;
		this.minBackoff = minBackoff;
		this.maxBackoff = maxBackoff;
		this.jitterFactor = jitterFactor;
		this.backoffScheduler = backoffScheduler;
		this.doPreRetry = doPreRetry;
		this.doPostRetry = doPostRetry;
		this.asyncPreRetry = asyncPreRetry;
		this.asyncPostRetry = asyncPostRetry;
	}

	/**
	 * Set the maximum number of retry attempts allowed. 1 meaning "1 retry attempt":
	 * the original subscription plus an extra re-subscription in case of an error, but
	 * no more.
	 *
	 * @param maxAttempts the new retry attempt limit
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder maxAttempts(long maxAttempts) {
		return new RetryBackoffBuilder(
				maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	/**
	 * Set the {@link Predicate} that will filter which errors can be retried. Exceptions
	 * that don't pass the predicate will be propagated downstream and terminate the retry
	 * sequence. Defaults to allowing retries for all exceptions.
	 *
	 * @param predicate the predicate to filter which exceptions can be retried
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder throwablePredicate(Predicate<? super Throwable> predicate) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				Objects.requireNonNull(predicate, "predicate"),
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	/**
	 * Allows to augment a previously {@link #throwablePredicate(Predicate) set} {@link Predicate} with
	 * a new condition to allow retries of some exception or not. This can typically be used with
	 * {@link Predicate#and(Predicate)} to combine existing predicate(s) with a new one.
	 * <p>
	 * For example:
	 * <pre><code>
	 * //given
	 * RetryBuilder retryTwiceIllegalArgument = Retry.max(2)
	 *     .throwablePredicate(e -> e instanceof IllegalArgumentException);
	 *
	 * RetryBuilder retryTwiceIllegalArgWithCause = retryTwiceIllegalArgument.throwablePredicate(old ->
	 *     old.and(e -> e.getCause() != null));
	 * </code></pre>
	 *
	 * @param predicateAdjuster a {@link Function} that returns a new {@link Predicate} given the
	 * currently in place {@link Predicate} (usually deriving from the old predicate).
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder throwablePredicateModifiedWith(
			Function<Predicate<Throwable>, Predicate<? super Throwable>> predicateAdjuster) {
		Objects.requireNonNull(predicateAdjuster, "predicateAdjuster");
		Predicate<? super Throwable> newPredicate = Objects.requireNonNull(predicateAdjuster.apply(this.throwablePredicate),
				"predicateAdjuster must return a new predicate");
		return new RetryBackoffBuilder(
				this.maxAttempts,
				newPredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	public RetryBackoffBuilder andDoBeforeRetry(
			Consumer<RetrySignal> doBeforeRetry) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry.andThen(doBeforeRetry),
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	public RetryBackoffBuilder andDoAfterRetry(Consumer<RetrySignal> doAfterRetry) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry.andThen(doAfterRetry),
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	public RetryBackoffBuilder andDelayRetryWith(
			Function<RetrySignal, Mono<Void>> doAsyncBeforeRetry) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				(rs, m) -> asyncPreRetry.apply(rs, m).then(doAsyncBeforeRetry.apply(rs)),
				this.asyncPostRetry);
	}

	public RetryBackoffBuilder andRetryThen(
			Function<RetrySignal, Mono<Void>> doAsyncAfterRetry) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				(rs, m) -> asyncPostRetry.apply(rs, m).then(doAsyncAfterRetry.apply(rs)));
	}

	/**
	 * Set the transient error mode, indicating that the strategy being built should use
	 * {@link RetrySignal#failureSubsequentIndex()} rather than {@link RetrySignal#failureTotalIndex()}.
	 * Transient errors are errors that could occur in bursts but are then recovered from by
	 * a retry (with one or more onNext signals) before another error occurs.
	 * <p>
	 * For a backoff based retry, the backoff is also computed based on the index within
	 * the burst, meaning the next error after a recovery will be retried with a {@link #minBackoff(Duration)} delay.
	 *
	 * @param isTransientErrors {@code true} to activate transient mode
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder transientErrors(boolean isTransientErrors) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	/**
	 * Set the minimum {@link Duration} for the first backoff. This method switches to an
	 * exponential backoff strategy if not already done so. Defaults to {@link Duration#ZERO}
	 * when the strategy was initially not a backoff one.
	 *
	 * @param minBackoff the minimum backoff {@link Duration}
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder minBackoff(Duration minBackoff) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	/**
	 * Set a hard maximum {@link Duration} for exponential backoffs. This method switches
	 * to an exponential backoff strategy with a zero minimum backoff if not already a backoff
	 * strategy. Defaults to {@code Duration.ofMillis(Long.MAX_VALUE)}.
	 *
	 * @param maxBackoff the maximum backoff {@link Duration}
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder maxBackoff(Duration maxBackoff) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				maxBackoff,
				this.jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	/**
	 * Set a jitter factor for exponential backoffs that adds randomness to each backoff. This can
	 * be helpful in reducing cascading failure due to retry-storms. This method switches to an
	 * exponential backoff strategy with a zero minimum backoff if not already a backoff strategy.
	 * Defaults to {@code 0.5} (a jitter of at most 50% of the computed delay).
	 *
	 * @param jitterFactor the new jitter factor as a {@code double} between {@code 0d} and {@code 1d}
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder jitter(double jitterFactor) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				jitterFactor,
				this.backoffScheduler,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	/**
	 * Set a {@link Scheduler} on which to execute the delays computed by the exponential backoff
	 * strategy. This method switches to an exponential backoff strategy with a zero minimum backoff
	 * if not already a backoff strategy. Defaults to {@link Schedulers#parallel()} in the backoff
	 * strategy.
	 *
	 * @param backoffScheduler the {@link Scheduler} to use
	 * @return the builder for further configuration
	 */
	public RetryBackoffBuilder scheduler(Scheduler backoffScheduler) {
		return new RetryBackoffBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.jitterFactor,
				Objects.requireNonNull(backoffScheduler, "backoffScheduler"),
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	//==========
	// strategy
	//==========

	protected void validateArguments() {
		if (jitterFactor < 0 || jitterFactor > 1) throw new IllegalArgumentException("jitterFactor must be between 0 and 1 (default 0.5)");
		Objects.requireNonNull(this.backoffScheduler, "backoffScheduler must not be null (default Schedulers.parallel())");
	}

	@Override
	public Publisher<?> generateCompanion(Flux<RetrySignal> t) {
		validateArguments();
		return t.flatMap(retryWhenState -> {
			//capture the state immediately
			RetrySignal copy = retryWhenState.retain();
			Throwable currentFailure = copy.failure();
			long iteration = isTransientErrors ? copy.failureSubsequentIndex() : copy.failureTotalIndex();

			if (currentFailure == null) {
				return Mono.error(new IllegalStateException("Retry.RetrySignal#failure() not expected to be null"));
			}

			if (!throwablePredicate.test(currentFailure)) {
				return Mono.error(currentFailure);
			}

			if (iteration >= maxAttempts) {
				return Mono.error(Exceptions.retryExhausted(maxAttempts, currentFailure));
			}

			Duration nextBackoff;
			try {
				nextBackoff = minBackoff.multipliedBy((long) Math.pow(2, iteration));
				if (nextBackoff.compareTo(maxBackoff) > 0) {
					nextBackoff = maxBackoff;
				}
			}
			catch (ArithmeticException overflow) {
				nextBackoff = maxBackoff;
			}

			//short-circuit delay == 0 case
			if (nextBackoff.isZero()) {
				return RetryBuilder.applyHooks(copy, Mono.just(iteration),
						doPreRetry, doPostRetry, asyncPreRetry, asyncPostRetry);
			}

			ThreadLocalRandom random = ThreadLocalRandom.current();

			long jitterOffset;
			try {
				jitterOffset = nextBackoff.multipliedBy((long) (100 * jitterFactor))
				                          .dividedBy(100)
				                          .toMillis();
			}
			catch (ArithmeticException ae) {
				jitterOffset = Math.round(Long.MAX_VALUE * jitterFactor);
			}
			long lowBound = Math.max(minBackoff.minus(nextBackoff)
			                                     .toMillis(), -jitterOffset);
			long highBound = Math.min(maxBackoff.minus(nextBackoff)
			                                    .toMillis(), jitterOffset);

			long jitter;
			if (highBound == lowBound) {
				if (highBound == 0) jitter = 0;
				else jitter = random.nextLong(highBound);
			}
			else {
				jitter = random.nextLong(lowBound, highBound);
			}
			Duration effectiveBackoff = nextBackoff.plusMillis(jitter);
			return RetryBuilder.applyHooks(copy, Mono.delay(effectiveBackoff, backoffScheduler),
						doPreRetry, doPostRetry, asyncPreRetry, asyncPostRetry);
		});
	}

}
