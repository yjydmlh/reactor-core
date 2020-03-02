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

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class RetryBuilderTest {

	@Test
	public void builderMethodsProduceNewInstances() {
		RetryBuilder init = Retry.max(1);
		assertThat(init)
				.isNotSameAs(init.maxAttempts(10))
				.isNotSameAs(init.throwablePredicate(t -> true))
				.isNotSameAs(init.throwablePredicateModifiedWith(predicate -> predicate.and(t -> true)))
				.isNotSameAs(init.transientErrors(true))
				.isNotSameAs(init.andDoBeforeRetry(rs -> {}))
				.isNotSameAs(init.andDoAfterRetry(rs -> {}))
				.isNotSameAs(init.andDelayRetryWith(rs -> Mono.empty()))
				.isNotSameAs(init.andRetryThen(rs -> Mono.empty()))
				.isNotSameAs(init.onRetryExhaustedThrow((b, rs) -> new IllegalStateException("boom")));
	}

	@Test
	public void builderCanBeUsedAsTemplate() {
		//a base builder can be reused across several Flux with different tuning for each flux
		RetryBuilder template = Retry.max(1).transientErrors(false);

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
				            .hasMessage("Retries exhausted: 2/2 (0 in a row)")
				            .hasCause(new IllegalStateException("boom 4")));

		StepVerifier.create(modifiedTemplate2, StepVerifierOptions.create().scenarioName("modified template 2"))
		            .expectNext(1, 3)
		            .verifyComplete();
	}

	@Test
	public void throwablePredicateReplacesThePredicate() {
		RetryBuilder retryBuilder = Retry.max(1)
		                                 .throwablePredicate(t -> t instanceof RuntimeException)
		                                 .throwablePredicate(t -> t instanceof IllegalStateException);

		assertThat(retryBuilder.throwablePredicate)
				.accepts(new IllegalStateException())
				.rejects(new IllegalArgumentException())
				.rejects(new RuntimeException());
	}

	@Test
	public void throwablePredicateModifierAugmentsThePredicate() {
		RetryBuilder retryBuilder = Retry.max(1)
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
		RetryBuilder retryBuilder = Retry.max(1)
		                                 .throwablePredicateModifiedWith(p -> p.and(t -> t.getMessage().length() == 3));

		assertThat(retryBuilder.throwablePredicate)
				.accepts(new IllegalStateException("foo"))
				.accepts(new IllegalArgumentException("bar"))
				.accepts(new RuntimeException("baz"))
				.rejects(new RuntimeException("too big"));
	}

	@Test
	public void throwablePredicateModifierRejectsNullGenerator() {
		assertThatNullPointerException().isThrownBy(() -> Retry.max(1).throwablePredicateModifiedWith(p -> null))
		                                .withMessage("predicateAdjuster must return a new predicate");
	}

	@Test
	public void throwablePredicateModifierRejectsNullFunction() {
		assertThatNullPointerException().isThrownBy(() -> Retry.max(1).throwablePredicateModifiedWith(null))
		                                .withMessage("predicateAdjuster");
	}

	@Test
	public void doBeforeRetryIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBuilder retryBuilder = Retry
				.max(1)
				.andDoBeforeRetry(rs -> atomic.incrementAndGet())
				.andDoBeforeRetry(rs -> atomic.addAndGet(100));

		retryBuilder.doPreRetry.accept(null);

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void doAfterRetryIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBuilder retryBuilder = Retry
				.max(1)
				.andDoAfterRetry(rs -> atomic.incrementAndGet())
				.andDoAfterRetry(rs -> atomic.addAndGet(100));

		retryBuilder.doPostRetry.accept(null);

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void delayRetryWithIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBuilder retryBuilder = Retry
				.max(1)
				.andDelayRetryWith(rs -> Mono.fromRunnable(atomic::incrementAndGet))
				.andDelayRetryWith(rs -> Mono.fromRunnable(() -> atomic.addAndGet(100)));

		retryBuilder.asyncPreRetry.apply(null, Mono.empty()).block();

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void retryThenIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBuilder retryBuilder = Retry
				.max(1)
				.andRetryThen(rs -> Mono.fromRunnable(atomic::incrementAndGet))
				.andRetryThen(rs -> Mono.fromRunnable(() -> atomic.addAndGet(100)));

		retryBuilder.asyncPostRetry.apply(null, Mono.empty()).block();

		assertThat(atomic).hasValue(101);
	}


	@Test
	public void retryExceptionDefaultsToRetryExhausted() {
		RetryBuilder retryBuilder = Retry.max(50);

		final ImmutableRetrySignal trigger = new ImmutableRetrySignal(100, 21, new IllegalStateException("boom"));

		StepVerifier.create(retryBuilder.generateCompanion(Flux.just(trigger)))
		            .expectErrorSatisfies(e -> assertThat(e).matches(Exceptions::isRetryExhausted, "isRetryExhausted")
		                                                    .hasMessage("Retries exhausted: 100/50 (21 in a row)")
		                                                    .hasCause(new IllegalStateException("boom")))
		            .verify();
	}

	@Test
	public void retryExceptionCanBeCustomized() {
		RetryBuilder retryBuilder = Retry
				.max(50)
				.onRetryExhaustedThrow((builder, rs) -> new IllegalArgumentException("max" + builder.maxAttempts));

		final ImmutableRetrySignal trigger = new ImmutableRetrySignal(100, 21, new IllegalStateException("boom"));

		StepVerifier.create(retryBuilder.generateCompanion(Flux.just(trigger)))
		            .expectErrorSatisfies(e -> assertThat(e).matches(t -> !Exceptions.isRetryExhausted(t), "is not retryExhausted")
		                                                    .hasMessage("max50")
		                                                    .hasNoCause())
		            .verify();
	}

	@Test
	public void defaultRetryExhaustedMessageWithNoTransientErrors() {
		assertThat(RetryBuilder.RETRY_EXCEPTION_GENERATOR.apply(Retry.max(123), new ImmutableRetrySignal(123, 123, null)))
				.hasMessage("Retries exhausted: 123/123");
	}

	@Test
	public void defaultRetryExhaustedMessageWithTransientErrors() {
		assertThat(RetryBuilder.RETRY_EXCEPTION_GENERATOR.apply(Retry.max(123), new ImmutableRetrySignal(123, 12, null)))
				.hasMessage("Retries exhausted: 123/123 (12 in a row)");
	}

}