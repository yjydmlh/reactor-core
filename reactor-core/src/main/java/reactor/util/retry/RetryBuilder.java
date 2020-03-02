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

import org.reactivestreams.Publisher;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A builder for a simple count-based retry strategy with fine grained options: errors that match
 * the {@link #throwablePredicate(Predicate)} are retried (by default all), up to {@link #maxAttempts(long)} times.
 * <p>
 * When the maximum attempt of retries is reached, a runtime exception is propagated downstream which
 * can be pinpointed with {@link reactor.core.Exceptions#isRetryExhausted(Throwable)}. The cause of
 * the last attempt's failure is attached as said {@link reactor.core.Exceptions#retryExhausted(long, Throwable) retryExhausted}
 * exception's cause.
 * <p>
 * Additionally, to help dealing with bursts of transient errors in a long-lived Flux as if each burst
 * had its own attempt counter, one can choose to set {@link #transientErrors(boolean)} to {@code true}.
 * The comparison to {@link #maxAttempts(long)} will then be done with the number of subsequent attempts
 * that failed without an {@link org.reactivestreams.Subscriber#onNext(Object) onNext} in between.
 * <p>
 * The {@link RetryBuilder} is copy-on-write and as such can be stored as a "template" and further configured
 * by different components without a risk of modifying the original configuration.
 *
 * @author Simon Baslé
 */
public final class RetryBuilder implements Retry {

	final long                 maxAttempts;
	final Predicate<Throwable> throwablePredicate;
	final boolean              isTransientErrors;

	final Consumer<Retry.RetrySignal>                           doPreRetry;
	final Consumer<Retry.RetrySignal>                           doPostRetry;
	final BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry;
	final BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry;

	/**
	 * Copy constructor.
	 */
	RetryBuilder(long max,
			Predicate<? super Throwable> aThrowablePredicate,
			boolean isTransientErrors,
			Consumer<Retry.RetrySignal> doPreRetry,
			Consumer<Retry.RetrySignal> doPostRetry,
			BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry,
			BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry) {
		this.maxAttempts = max;
		this.throwablePredicate = aThrowablePredicate::test; //massaging type
		this.isTransientErrors = isTransientErrors;
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
	public RetryBuilder maxAttempts(long maxAttempts) {
		return new RetryBuilder(
				maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
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
	public RetryBuilder throwablePredicate(Predicate<? super Throwable> predicate) {
		return new RetryBuilder(
				this.maxAttempts,
				Objects.requireNonNull(predicate, "predicate"),
				this.isTransientErrors,
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
	public RetryBuilder throwablePredicateModifiedWith(
			Function<Predicate<Throwable>, Predicate<? super Throwable>> predicateAdjuster) {
		Objects.requireNonNull(predicateAdjuster, "predicateAdjuster");
		Predicate<? super Throwable> newPredicate = Objects.requireNonNull(predicateAdjuster.apply(this.throwablePredicate),
				"predicateAdjuster must return a new predicate");
		return new RetryBuilder(
				this.maxAttempts,
				newPredicate,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	public RetryBuilder andDoBeforeRetry(
			Consumer<Retry.RetrySignal> doBeforeRetry) {
		return new RetryBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.doPreRetry.andThen(doBeforeRetry),
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	public RetryBuilder andDoAfterRetry(Consumer<Retry.RetrySignal> doAfterRetry) {
		return new RetryBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry.andThen(doAfterRetry),
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	public RetryBuilder andDelayRetryWith(
			Function<Retry.RetrySignal, Mono<Void>> doAsyncBeforeRetry) {
		return new RetryBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
				(rs, m) -> asyncPreRetry.apply(rs, m).then(doAsyncBeforeRetry.apply(rs)),
				this.asyncPostRetry);
	}

	public RetryBuilder andRetryThen(
			Function<Retry.RetrySignal, Mono<Void>> doAsyncAfterRetry) {
		return new RetryBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				this.isTransientErrors,
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
	 * In the case of a simple count-based retry, this means that the {@link #maxAttempts(long)}
	 * is applied to each burst individually.
	 *
	 * @param isTransientErrors {@code true} to activate transient mode
	 * @return the builder for further configuration
	 */
	public RetryBuilder transientErrors(boolean isTransientErrors) {
		return new RetryBuilder(
				this.maxAttempts,
				this.throwablePredicate,
				isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry);
	}

	//==========
	// strategy
	//==========

	@Override
	public Publisher<?> generateCompanion(Flux<RetrySignal> flux) {
		return flux.flatMap(retryWhenState -> {
			//capture the state immediately
			Retry.RetrySignal copy = retryWhenState.retain();
			Throwable currentFailure = copy.failure();
			long iteration = isTransientErrors ? copy.failureSubsequentIndex() : copy.failureTotalIndex();

			if (currentFailure == null) {
				return Mono.error(new IllegalStateException("RetryWhenState#failure() not expected to be null"));
			}
			else if (!throwablePredicate.test(currentFailure)) {
				return Mono.error(currentFailure);
			}
			else if (iteration >= maxAttempts) {
				return Mono.error(Exceptions.retryExhausted(maxAttempts, currentFailure));
			}
			else {
				return applyHooks(copy, Mono.just(iteration), doPreRetry, doPostRetry, asyncPreRetry, asyncPostRetry);
			}
		});
	}

	//===================
	// utility functions
	//===================

	static final Duration                                        MAX_BACKOFF      = Duration.ofMillis(Long.MAX_VALUE);
	static final Consumer<RetrySignal>                           NO_OP_CONSUMER   = rs -> {};
	static final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> NO_OP_BIFUNCTION = (rs, m) -> m;

	static <T> Mono<T> applyHooks(Retry.RetrySignal copyOfSignal,
			Mono<T> originalCompanion,
			final Consumer<Retry.RetrySignal> doPreRetry,
			final Consumer<Retry.RetrySignal> doPostRetry,
			final BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry,
			final BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry) {
		if (doPreRetry != NO_OP_CONSUMER) {
			try {
				doPreRetry.accept(copyOfSignal);
			}
			catch (Throwable e) {
				return Mono.error(e);
			}
		}

		Mono<Void> postRetrySyncMono;
		if (doPostRetry != NO_OP_CONSUMER) {
			postRetrySyncMono = Mono.fromRunnable(() -> doPostRetry.accept(copyOfSignal));
		}
		else {
			postRetrySyncMono = Mono.empty();
		}

		Mono<Void> preRetryMono = asyncPreRetry == NO_OP_BIFUNCTION ? Mono.empty() : asyncPreRetry.apply(copyOfSignal, Mono.empty());
		Mono<Void> postRetryMono = asyncPostRetry != NO_OP_BIFUNCTION ? asyncPostRetry.apply(copyOfSignal, postRetrySyncMono) : postRetrySyncMono;

		return preRetryMono.then(originalCompanion).flatMap(postRetryMono::thenReturn);
	}
}
