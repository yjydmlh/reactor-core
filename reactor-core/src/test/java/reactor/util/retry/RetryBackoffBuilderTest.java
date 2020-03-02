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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class RetryBackoffBuilderTest {

	@Test
	public void suppressingSchedulerFails() {
		assertThatNullPointerException().isThrownBy(() -> Retry.backoff(1, Duration.ZERO).scheduler(null))
		                                .withMessage("backoffScheduler");
	}

	@Test
	public void builderMethodsProduceNewInstances() {
		RetryBackoffBuilder init = Retry.backoff(1, Duration.ZERO);
		assertThat(init)
				.isNotSameAs(init.minBackoff(Duration.ofSeconds(1)))
				.isNotSameAs(init.maxBackoff(Duration.ZERO))
				.isNotSameAs(init.jitter(0.5d))
				.isNotSameAs(init.scheduler(Schedulers.parallel()))
				.isNotSameAs(init.maxAttempts(10))
				.isNotSameAs(init.throwablePredicate(t -> true))
				.isNotSameAs(init.throwablePredicateModifiedWith(predicate -> predicate.and(t -> true)))
				.isNotSameAs(init.transientErrors(true))
				.isNotSameAs(init.andDoBeforeRetry(rs -> {}))
				.isNotSameAs(init.andDoAfterRetry(rs -> {}))
				.isNotSameAs(init.andDelayRetryWith(rs -> Mono.empty()))
				.isNotSameAs(init.andRetryThen(rs -> Mono.empty()));
	}

	@Test
	public void builderCanBeUsedAsTemplate() {
		//a base builder can be reused across several Flux with different tuning for each flux
		RetryBackoffBuilder template = Retry.backoff(1, Duration.ZERO).transientErrors(false);

		Supplier<Flux<Integer>> transientError = () -> {
			AtomicInteger errorOnEven = new AtomicInteger();
			return Flux.generate(sink -> {
				int i = errorOnEven.getAndIncrement();
				if (i == 5) {
					sink.complete();
				}
				if (i % 2 == 0) {
					sink.error(new IllegalStateException("boom " + i));
				}
				else {
					sink.next(i);
				}
			});
		};

		Flux<Integer> modifiedTemplate1 = transientError.get().retryWhen(template.maxAttempts(2));
		Flux<Integer> modifiedTemplate2 = transientError.get().retryWhen(template.transientErrors(true));

		StepVerifier.create(modifiedTemplate1, StepVerifierOptions.create().scenarioName("modified template 1"))
		            .expectNext(1, 3)
		            .verifyErrorSatisfies(t -> assertThat(t)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("Retries exhausted: 2/2")
				            .hasCause(new IllegalStateException("boom 4")));

		StepVerifier.create(modifiedTemplate2, StepVerifierOptions.create().scenarioName("modified template 2"))
		            .expectNext(1, 3)
		            .verifyComplete();
	}

	@Test
	public void throwablePredicateReplacesThePredicate() {
		RetryBackoffBuilder retryBuilder = Retry.backoff(1, Duration.ZERO)
		                                 .throwablePredicate(t -> t instanceof RuntimeException)
		                                 .throwablePredicate(t -> t instanceof IllegalStateException);

		assertThat(retryBuilder.throwablePredicate)
				.accepts(new IllegalStateException())
				.rejects(new IllegalArgumentException())
				.rejects(new RuntimeException());
	}

	@Test
	public void throwablePredicateModifierAugmentsThePredicate() {
		RetryBackoffBuilder retryBuilder = Retry.backoff(1, Duration.ZERO)
		                                 .throwablePredicate(t -> t instanceof RuntimeException)
		                                 .throwablePredicateModifiedWith(p -> p.and(t -> t.getMessage().length() == 3));

		assertThat(retryBuilder.throwablePredicate)
				.accepts(new IllegalStateException("foo"))
				.accepts(new IllegalArgumentException("bar"))
				.accepts(new RuntimeException("baz"))
				.rejects(new RuntimeException("too big"));
	}

	@Test
	public void throwablePredicateModifierWorksIfNoPreviousPredicate() {
		RetryBackoffBuilder retryBuilder = Retry.backoff(1, Duration.ZERO)
		                                 .throwablePredicateModifiedWith(p -> p.and(t -> t.getMessage().length() == 3));

		assertThat(retryBuilder.throwablePredicate)
				.accepts(new IllegalStateException("foo"))
				.accepts(new IllegalArgumentException("bar"))
				.accepts(new RuntimeException("baz"))
				.rejects(new RuntimeException("too big"));
	}

	@Test
	public void throwablePredicateModifierRejectsNullGenerator() {
		assertThatNullPointerException().isThrownBy(() -> Retry.backoff(1, Duration.ZERO).throwablePredicateModifiedWith(p -> null))
		                                .withMessage("predicateAdjuster must return a new predicate");
	}

	@Test
	public void throwablePredicateModifierRejectsNullFunction() {
		assertThatNullPointerException().isThrownBy(() -> Retry.backoff(1, Duration.ZERO).throwablePredicateModifiedWith(null))
		                                .withMessage("predicateAdjuster");
	}

	@Test
	public void doBeforeRetryIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffBuilder retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.andDoBeforeRetry(rs -> atomic.incrementAndGet())
				.andDoBeforeRetry(rs -> atomic.addAndGet(100));

		retryBuilder.doPreRetry.accept(null);

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void doAfterRetryIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffBuilder retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.andDoAfterRetry(rs -> atomic.incrementAndGet())
				.andDoAfterRetry(rs -> atomic.addAndGet(100));

		retryBuilder.doPostRetry.accept(null);

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void delayRetryWithIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffBuilder retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.andDelayRetryWith(rs -> Mono.fromRunnable(atomic::incrementAndGet))
				.andDelayRetryWith(rs -> Mono.fromRunnable(() -> atomic.addAndGet(100)));

		retryBuilder.asyncPreRetry.apply(null, Mono.empty()).block();

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void retryThenIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffBuilder retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.andRetryThen(rs -> Mono.fromRunnable(atomic::incrementAndGet))
				.andRetryThen(rs -> Mono.fromRunnable(() -> atomic.addAndGet(100)));

		retryBuilder.asyncPostRetry.apply(null, Mono.empty()).block();

		assertThat(atomic).hasValue(101);
	}

}