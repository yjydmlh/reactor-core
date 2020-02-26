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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class SimpleRetryFunction implements Retry {

	static final Duration                                        MAX_BACKOFF      = Duration.ofMillis(Long.MAX_VALUE);
	static final Consumer<RetrySignal>                           NO_OP_CONSUMER   = rs -> {};
	static final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> NO_OP_BIFUNCTION = (rs, m) -> m;

	final long                                                  maxAttempts;
	final Predicate<Throwable>                                  throwablePredicate;
	final boolean                                               isTransientErrors;
	final Consumer<Retry.RetrySignal>                           doPreRetry;
	final Consumer<Retry.RetrySignal>                           doPostRetry;
	final BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry;
	final BiFunction<Retry.RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry;

	SimpleRetryFunction(Retry.Builder builder) {
		this.maxAttempts = builder.maxAttempts;
		this.throwablePredicate = builder.throwablePredicate;
		this.isTransientErrors = builder.isTransientErrors;
		this.doPreRetry = builder.doPreRetry;
		this.doPostRetry = builder.doPostRetry;
		this.asyncPreRetry = builder.asyncPreRetry;
		this.asyncPostRetry= builder.asyncPostRetry;
	}

	@Override
	public Publisher<?> generateCompanion(Flux<Retry.RetrySignal> flux) {
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
				return applyHooks(copy, Mono.just(iteration));
			}
		});
	}

	protected <T> Mono<T> applyHooks(Retry.RetrySignal copyOfSignal, Mono<T> originalCompanion) {
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
