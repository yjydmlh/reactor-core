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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

/**
 * Utilities around {@link Flux#retry(Supplier)}  retries} (builder to configure retries,
 * retry {@link RetrySignal signal}, etc...)
 *
 * @author Simon Baslé
 */
public class Retry {

	static final Duration MAX_BACKOFF = Duration.ofMillis(Long.MAX_VALUE);

	static final Consumer<RetrySignal>                           NO_OP_CONSUMER   = rs -> {};
	static final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> NO_OP_BIFUNCTION = (rs, m) -> m;

	/**
	 * State for a {@link Flux#retry(Supplier) Flux retry} or {@link reactor.core.publisher.Mono#retry(Supplier) Mono retry}.
	 * The state is passed to the retry function inside a publisher and gives information about the
	 * {@link #failure()} that potentially triggers a retry, as well as two indexes: the number of
	 * errors that happened so far (and were retried) and the same number, but only taking into account
	 * <strong>subsequent</strong> errors (see {@link #failureSubsequentIndex()}).
	 */
	public interface RetrySignal {

		/**
		 * The ZERO BASED index number of this error (can also be read as how many retries have occurred
		 * so far), since the source was first subscribed to.
		 *
		 * @return a 0-index for the error, since original subscription
		 */
		long failureTotalIndex();

		/**
		 * The ZERO BASED index number of this error since the beginning of the current burst of errors.
		 * This is reset to zero whenever a retry is made that is followed by at least one
		 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext}.
		 *
		 * @return a 0-index for the error in the current burst of subsequent errors
		 */
		long failureSubsequentIndex();

		/**
		 * The current {@link Throwable} that needs to be evaluated for retry.
		 *
		 * @return the current failure {@link Throwable}
		 */
		Throwable failure();

		/**
		 * If this {@link RetrySignal} is a transient view of the state of the underlying retry subscriber,
		 * return an immutable copy of that view that is guaranteed to be consistent with the time at which
		 * this method is invoked.
		 *
		 * @return an immutable copy of the current {@link RetrySignal}, always safe to use
		 */
		default RetrySignal retain() {
			return new ImmutableRetrySignal(failureTotalIndex(), failureSubsequentIndex(), failure());
		}
	}

	/**
	 * A {@link Builder} preconfigured for exponential backoff strategy with jitter, given a maximum number of retry attempts
	 * and a minimum {@link Duration} for the backoff.
	 *
	 * @param maxAttempts the maximum number of retry attempts to allow
	 * @param minBackoff the minimum {@link Duration} for the first backoff
	 * @return the builder for further configuration
	 * @see Builder#maxAttempts(long)
	 * @see Builder#minBackoff(Duration)
	 */
	public static Builder backoff(long maxAttempts, Duration minBackoff) {
		return new Builder(true, maxAttempts, t -> true, false, minBackoff, MAX_BACKOFF, 0.5d, Schedulers.parallel(),
				NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION);
	}

	/**
	 * A {@link Builder} preconfigured for a simple strategy with maximum number of retry attempts.
	 *
	 * @param max the maximum number of retry attempts to allow
	 * @return the builder for further configuration
	 * @see Builder#maxAttempts(long)
	 */
	public static Builder max(long max) {
		return new Builder(false, max, t -> true, false, Duration.ZERO, MAX_BACKOFF, 0d,null,
				NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION);
	}

	/**
	 * A {@link Builder} preconfigured for a simple strategy with maximum number of retry attempts over
	 * subsequent transient errors. An {@link org.reactivestreams.Subscriber#onNext(Object)} between
	 * errors resets the counter (see {@link Builder#transientErrors(boolean)}).
	 *
	 * @param maxInARow the maximum number of retry attempts to allow in a row, reset by successful onNext
	 * @return the builder for further configuration
	 * @see Builder#maxAttempts(long)
	 * @see Builder#transientErrors(boolean)
	 */
	public static Builder maxInARow(long maxInARow) {
		return new Builder(false, maxInARow, t -> true, true, Duration.ZERO, MAX_BACKOFF, 0d,null,
				NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION);
	}

	/**
	 * A builder for a retry strategy with fine grained options.
	 * <p>
	 * By default the strategy is simple: errors that match the {@link #throwablePredicate(Predicate)}
	 * (by default all) are retried up to {@link #maxAttempts(long)} times.
	 * <p>
	 * If one of the {@link #minBackoff(Duration)}, {@link #maxBackoff(Duration)}, {@link #jitter(double)}
	 * or {@link #scheduler(Scheduler)} method is used, the strategy becomes an exponential backoff strategy,
	 * randomized with a user-provided {@link #jitter(double)} factor between {@code 0.d} (no jitter)
	 * and {@code 1.0} (default is {@code 0.5}).
	 * Even with the jitter, the effective backoff delay cannot be less than {@link #minBackoff(Duration)}
	 * nor more than {@link #maxBackoff(Duration)}. The delays and subsequent attempts are executed on the
	 * provided backoff {@link #scheduler(Scheduler)}.
	 * <p>
	 * Additionally, to help dealing with bursts of transient errors in a long-lived Flux as if each burst
	 * had its own backoff, one can choose to set {@link #transientErrors(boolean)} to {@code true}.
	 * The comparison to {@link #maxAttempts(long)} will then be done with the number of subsequent attempts
	 * that failed without an {@link org.reactivestreams.Subscriber#onNext(Object) onNext} in between.
	 * <p>
	 * The {@link Builder} is copy-on-write and as such can be stored as a "template" and further configured
	 * by different components without a risk of modifying the original configuration.
	 */
	public static class Builder implements Supplier<Function<Flux<RetrySignal>, Publisher<?>>> {

		final Duration  minBackoff;
		final Duration  maxBackoff;
		final double    jitterFactor;
		@Nullable
		final Scheduler backoffScheduler;

		final long                 maxAttempts;
		final Predicate<Throwable> throwablePredicate;
		final boolean              isTransientErrors;
		final boolean              isConfiguredForBackoff;

		final Consumer<RetrySignal> doPreRetry;
		final Consumer<RetrySignal> doPostRetry;
		final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry;
		final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry;

		/**
		 * Copy constructor.
		 */
		Builder(boolean isConfiguredForBackoff, long max,
				Predicate<? super Throwable> aThrowablePredicate,
				boolean isTransientErrors,
				Duration minBackoff, Duration maxBackoff, double jitterFactor,
				@Nullable Scheduler backoffScheduler,
				Consumer<RetrySignal> doPreRetry,
				Consumer<RetrySignal> doPostRetry,
				BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry,
				BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry) {
			this.isConfiguredForBackoff = isConfiguredForBackoff;
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
		 * Is this {@link Retry.Builder} configured for backing off (ie. have any of the
		 * {@link #minBackoff(Duration)}, {@link #maxBackoff(Duration)}, {@link #jitter(double)}
		 * or {@link #scheduler(Scheduler)} methods been called)?
		 *
		 * @return true if builder is going to build a backoff function, false for a simple retry function
		 */
		public boolean isConfiguredForBackoff() {
			return this.isConfiguredForBackoff;
		}

		/**
		 * Set the maximum number of retry attempts allowed. 1 meaning "1 retry attempt":
		 * the original subscription plus an extra re-subscription in case of an error, but
		 * no more.
		 *
		 * @param maxAttempts the new retry attempt limit
		 * @return the builder for further configuration
		 */
		public Builder maxAttempts(long maxAttempts) {
			return new Builder(
					this.isConfiguredForBackoff,
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
		public Builder throwablePredicate(Predicate<? super Throwable> predicate) {
			return new Builder(
					this.isConfiguredForBackoff,
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
		 * Builder retryTwiceIllegalArgument = Retry.max(2)
		 *     .throwablePredicate(e -> e instanceof IllegalArgumentException);
		 *
		 * Builder retryTwiceIllegalArgWithCause = retryTwiceIllegalArgument.throwablePredicate(old ->
		 *     old.and(e -> e.getCause() != null));
		 * </code></pre>
		 *
		 * @param predicateAdjuster a {@link Function} that returns a new {@link Predicate} given the
		 * currently in place {@link Predicate} (usually deriving from the old predicate).
		 * @return the builder for further configuration
		 */
		public Builder throwablePredicateModifiedWith(
				Function<Predicate<Throwable>, Predicate<? super Throwable>> predicateAdjuster) {
			Objects.requireNonNull(predicateAdjuster, "predicateAdjuster");
			Predicate<? super Throwable> newPredicate = Objects.requireNonNull(predicateAdjuster.apply(this.throwablePredicate),
					"predicateAdjuster must return a new predicate");
			return new Builder(
					this.isConfiguredForBackoff,
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

		public Builder andDoBeforeRetry(Consumer<RetrySignal> doBeforeRetry) {
			return new Builder(
					this.isConfiguredForBackoff,
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

		public Builder andDoAfterRetry(Consumer<RetrySignal> doAfterRetry) {
			return new Builder(
					this.isConfiguredForBackoff,
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

		public Builder andDelayRetryWith(Function<RetrySignal, Mono<Void>> doAsyncBeforeRetry) {
			return new Builder(
					this.isConfiguredForBackoff,
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

		public Builder andRetryThen(Function<RetrySignal, Mono<Void>> doAsyncAfterRetry) {
			return new Builder(
					this.isConfiguredForBackoff,
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
		 * In simplified mode, this means that the {@link #maxAttempts(long)} is applied
		 * to each burst individually. In exponential backoff, the backoff is also computed
		 * based on the index within the burst, meaning the next error after a recovery will
		 * be retried with a {@link #minBackoff(Duration)} delay.
		 *
		 * @param isTransientErrors {@code true} to activate transient mode
		 * @return the builder for further configuration
		 */
		public Builder transientErrors(boolean isTransientErrors) {
			return new Builder(
					this.isConfiguredForBackoff,
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
		public Builder minBackoff(Duration minBackoff) {
			return new Builder(
					true,
					this.maxAttempts,
					this.throwablePredicate,
					this.isTransientErrors,
					Objects.requireNonNull(minBackoff, "minBackoff"),
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
		public Builder maxBackoff(Duration maxBackoff) {
			return new Builder(
					true,
					this.maxAttempts,
					this.throwablePredicate,
					this.isTransientErrors,
					this.minBackoff,
					Objects.requireNonNull(maxBackoff, "maxBackoff"),
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
		public Builder jitter(double jitterFactor) {
			return new Builder(
					true,
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
		public Builder scheduler(Scheduler backoffScheduler) {
			return new Builder(
					true,
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

		/**
		 * Build the configured retry strategy.
		 *
		 * @return the retry {@link Function} based on a companion flux of {@link RetrySignal}
		 */
		@Override
		public Function<Flux<RetrySignal>, Publisher<?>> get() {
			if (isConfiguredForBackoff) {
				return new ExponentialBackoffFunction(this);
			}
			return new SimpleRetryFunction(this);
		}
	}

	static class ImmutableRetrySignal implements RetrySignal {

		final long failureTotalIndex;
		final long failureSubsequentIndex;
		final Throwable failure;

		ImmutableRetrySignal(long failureTotalIndex, long failureSubsequentIndex,
				Throwable failure) {
			this.failureTotalIndex = failureTotalIndex;
			this.failureSubsequentIndex = failureSubsequentIndex;
			this.failure = failure;
		}

		@Override
		public long failureTotalIndex() {
			return this.failureTotalIndex;
		}

		@Override
		public long failureSubsequentIndex() {
			return this.failureSubsequentIndex;
		}

		@Override
		public Throwable failure() {
			return this.failure;
		}

		@Override
		public RetrySignal retain() {
			return this;
		}

		@Override
		public String toString() {
			return "attempt #" + (failureTotalIndex + 1) + " (" + (failureSubsequentIndex + 1) + " in a row), last failure={" + failure + '}';
		}
	}
}
