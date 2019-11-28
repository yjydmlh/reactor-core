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

import org.assertj.core.api.Condition;
import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class RetryBuilderTest {


	private static final Condition<Retry.Builder> CONFIGURED_FOR_BACKOFF =
			new Condition<>(Retry.Builder::isConfiguredForBackoff,
					"isConfiguredForBackoff");

	private static final Condition<Retry.Builder> PRODUCING_BACKOFF_FUNCTION =
			new Condition<>(b -> b.get() instanceof ExponentialBackoffFunction,
					"produces backoff Function");

	@Test
	public void callingMinBackoffSwitchesToBackoffFunction() {
		Retry.Builder builder = Retry.max(1);
		assertThat(builder.get()).as("smoke test initially SimpleRetryFunction").isInstanceOf(SimpleRetryFunction.class);

		assertThat(builder.minBackoff(Duration.ofMillis(50)))
				.as("minBackoff(50ms)")
				.is(CONFIGURED_FOR_BACKOFF)
				.is(PRODUCING_BACKOFF_FUNCTION);

		assertThat(builder.minBackoff(Duration.ZERO))
				.as("minBackoff(ZERO)")
				.is(CONFIGURED_FOR_BACKOFF)
				.is(PRODUCING_BACKOFF_FUNCTION);

		assertThat(builder.isConfiguredForBackoff()).as("original not configured for backoff").isFalse();
	}

	@Test
	public void callingMaxBackoffSwitchesToBackoffFunction() {
		Retry.Builder builder = Retry.max(1);
		assertThat(builder.get()).as("smoke test initially SimpleRetryFunction").isInstanceOf(SimpleRetryFunction.class);

		assertThat(builder.maxBackoff(Duration.ofMillis(50)))
				.as("maxBackoff(50ms)")
				.is(CONFIGURED_FOR_BACKOFF)
				.is(PRODUCING_BACKOFF_FUNCTION);

		assertThat(builder.maxBackoff(Retry.MAX_BACKOFF))
				.as("maxBackoff(MAX)")
				.is(CONFIGURED_FOR_BACKOFF)
				.is(PRODUCING_BACKOFF_FUNCTION);
	}

	@Test
	public void callingJitterSwitchesToBackoffFunction() {
		Retry.Builder builder = Retry.max(1);
		assertThat(builder.get()).as("smoke test initially SimpleRetryFunction").isInstanceOf(SimpleRetryFunction.class);

		assertThat(builder.jitter(0.5d))
				.as("jitter(0.5d)")
				.is(CONFIGURED_FOR_BACKOFF)
				.is(PRODUCING_BACKOFF_FUNCTION);

		assertThat(builder.jitter(0d))
				.as("jitter(0d)")
				.is(CONFIGURED_FOR_BACKOFF)
				.is(PRODUCING_BACKOFF_FUNCTION);
	}

	@Test
	public void callingSchedulerSwitchesToBackoffFunction() {
		Retry.Builder builder = Retry.max(1);
		assertThat(builder.get()).as("smoke test initially SimpleRetryFunction").isInstanceOf(SimpleRetryFunction.class);

		assertThat(builder.scheduler(Schedulers.parallel()))
				.as("scheduler(parallel)")
				.is(CONFIGURED_FOR_BACKOFF)
				.is(PRODUCING_BACKOFF_FUNCTION);
	}

	@Test
	public void suppressingSchedulerFails() {
		assertThatNullPointerException().isThrownBy(() -> Retry.backoff(1, Duration.ZERO).scheduler(null))
		                                .withMessage("backoffScheduler");
	}

	@Test
	public void builderMethodsProduceNewInstances() {
		Retry.Builder init = Retry.max(1);
		assertThat(init)
				.isNotSameAs(init.minBackoff(Duration.ZERO))
				.isNotSameAs(init.maxBackoff(Duration.ZERO))
				.isNotSameAs(init.jitter(0.5d))
				.isNotSameAs(init.scheduler(Schedulers.parallel()))
				.isNotSameAs(init.maxAttempts(10))
				.isNotSameAs(init.throwablePredicate(t -> true))
				.isNotSameAs(init.throwablePredicateModifiedWith(predicate -> predicate.and(t -> true)))
				.isNotSameAs(init.transientErrors(true));
	}

	@Test
	public void builderCanBeUsedAsTemplate() {
		//a base builder can be reused across several Flux with different tuning for each flux
		Retry.Builder template = Retry.max(1).transientErrors(false);

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

		Flux<Integer> modifiedTemplate1 = transientError.get().retry(template.maxAttempts(2));
		Flux<Integer> modifiedTemplate2 = transientError.get().retry(template.transientErrors(true));

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
		Retry.Builder builder = Retry.max(1)
		                             .throwablePredicate(t -> t instanceof RuntimeException)
		                             .throwablePredicate(t -> t instanceof IllegalStateException);

		assertThat(builder.throwablePredicate)
				.accepts(new IllegalStateException())
				.rejects(new IllegalArgumentException())
				.rejects(new RuntimeException());
	}

	@Test
	public void throwablePredicateModifierAugmentsThePredicate() {
		Retry.Builder builder = Retry.max(1)
		                             .throwablePredicate(t -> t instanceof RuntimeException)
		                             .throwablePredicateModifiedWith(p -> p.and(t -> t.getMessage().length() == 3));

		assertThat(builder.throwablePredicate)
				.accepts(new IllegalStateException("foo"))
				.accepts(new IllegalArgumentException("bar"))
				.accepts(new RuntimeException("baz"))
				.rejects(new RuntimeException("too big"));
	}

	@Test
	public void throwablePredicateModifierWorksIfNoPreviousPredicate() {
		Retry.Builder builder = Retry.max(1)
		                             .throwablePredicateModifiedWith(p -> p.and(t -> t.getMessage().length() == 3));

		assertThat(builder.throwablePredicate)
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

}