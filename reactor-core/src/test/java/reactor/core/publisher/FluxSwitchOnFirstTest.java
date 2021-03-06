/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

public class FluxSwitchOnFirstTest {

    @Test
    public void shouldNotSubscribeTwice() {
        Throwable[] throwables = new Throwable[1];
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                    .switchOnFirst((s, f) -> {
                        RaceTestUtils.race(
                            () -> f.subscribe(__ -> {}, t -> {
                                throwables[0] = t;
                                latch.countDown();
                            },  latch::countDown),
                            () -> f.subscribe(__ -> {}, t -> {
                                throwables[0] = t;
                                latch.countDown();
                            },  latch::countDown)
                        );

                        return Flux.empty();
                    }))
                    .expectSubscription()
                    .expectComplete()
                    .verify();


        Assertions.assertThat(throwables[0])
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessage("FluxSwitchOnFirst allows only one Subscriber");
    }

    @Test
    public void shouldNotSubscribeTwiceConditional() {
        Throwable[] throwables = new Throwable[1];
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    RaceTestUtils.race(
                                            () -> f.subscribe(__ -> {}, t -> {
                                                throwables[0] = t;
                                                latch.countDown();
                                            },  latch::countDown),
                                            () -> f.subscribe(__ -> {}, t -> {
                                                throwables[0] = t;
                                                latch.countDown();
                                            },  latch::countDown)
                                    );

                                    return Flux.empty();
                                })
                                .filter(e -> true)
                    )
                    .expectSubscription()
                    .expectComplete()
                    .verify();


        Assertions.assertThat(throwables[0])
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessage("FluxSwitchOnFirst allows only one Subscriber");
    }

    @Test
    public void shouldNotSubscribeTwiceWhenCanceled() {
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .doOnComplete(() -> {
                                    try {
                                        latch.await();
                                    }
                                    catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                                .hide()
                                .publishOn(Schedulers.parallel())
                                .cancelOn(NoOpsScheduler.INSTANCE)
                                .doOnCancel(latch::countDown)
                                .switchOnFirst((s, f) -> f)
                                .doOnSubscribe(s ->
                                    Schedulers
                                            .single()
                                            .schedule(s::cancel, 10, TimeUnit.MILLISECONDS)
                                )
        )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(200))
                    .thenCancel()
                    .verifyThenAssertThat()
                    .hasNotDroppedErrors();
    }

    @Test
    public void shouldNotSubscribeTwiceConditionalWhenCanceled() {
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .doOnComplete(() -> {
                                    try {
                                        latch.await();
                                    }
                                    catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                                .hide()
                                .publishOn(Schedulers.parallel())
                                .cancelOn(NoOpsScheduler.INSTANCE)
                                .doOnCancel(latch::countDown)
                                .switchOnFirst((s, f) -> f)
                                .filter(e -> true)
                                .doOnSubscribe(s ->
                                    Schedulers
                                        .single()
                                        .schedule(s::cancel, 10, TimeUnit.MILLISECONDS)
                                )
        )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(200))
                    .thenCancel()
                    .verifyThenAssertThat()
                    .hasNotDroppedErrors();
    }

    @Test
    public void shouldSendOnErrorSignalConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                .switchOnFirst((s, f) -> {
                    first[0] = s;

                    return f;
                })
                .filter(e -> true)
        )
                    .expectSubscription()
                    .expectError(RuntimeException.class)
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldSendOnNextSignalConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f;
                                })
                                .filter(e -> true)
                    )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldSendOnErrorSignalWithDelaySubscription() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                .switchOnFirst((s, f) -> {
                    first[0] = s;

                    return f.delaySubscription(Duration.ofMillis(100));
                }))
                    .expectSubscription()
                    .expectError(RuntimeException.class)
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldSendOnCompleteSignalWithDelaySubscription() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.<Long>empty()
                .switchOnFirst((s, f) -> {
                    first[0] = s;

                    return f.delaySubscription(Duration.ofMillis(100));
                }))
                    .expectSubscription()
                    .expectComplete()
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.complete());
    }

    @Test
    public void shouldSendOnErrorSignal() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f;
                                }))
                    .expectSubscription()
                    .expectError(RuntimeException.class)
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldSendOnNextSignal() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f;
                                }))
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }


    @Test
    public void shouldSendOnNextAsyncSignal() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f.subscribeOn(Schedulers.elastic());
                                }))
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldSendOnNextAsyncSignalConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f.subscribeOn(Schedulers.elastic());
                                })
                                .filter(p -> true)
                    )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldNeverSendIncorrectRequestSizeToUpstream() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        AtomicLong capture = new AtomicLong();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribeWith(new LambdaSubscriber<>(capture::set, __ -> {}, latch::countDown, s -> s.request(1)));

        latch.await(5, TimeUnit.SECONDS);

        Assertions.assertThat(capture.get()).isEqualTo(1L);
        Assertions.assertThat(requested).containsExactly(1L);
    }

    @Test
    public void shouldNeverSendIncorrectRequestSizeToUpstreamConditional() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        AtomicLong capture = new AtomicLong();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux)
                                                .filter(e -> true);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribeWith(new LambdaSubscriber<>(capture::set, __ -> {}, latch::countDown, s -> s.request(1)));

        latch.await(5, TimeUnit.SECONDS);

        Assertions.assertThat(capture.get()).isEqualTo(1L);
        Assertions.assertThat(requested).containsExactly(1L);
    }

    @Test
    public void shouldBeRequestedOneFromUpstreamTwiceInCaseOfConditional() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        ArrayList<Long> capture = new ArrayList<>();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux)
                                                .filter(e -> false);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribeWith(new LambdaSubscriber<>(capture::add, __ -> {}, latch::countDown, s -> s.request(1)));

        latch.await(5, TimeUnit.SECONDS);

        Assertions.assertThat(capture).isEmpty();
        Assertions.assertThat(requested).containsExactly(1L, 1L);
    }

    @Test
    public void shouldBeRequestedExactlyOneAndThenLongMaxValue() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        ArrayList<Long> capture = new ArrayList<>();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribe(capture::add, __ -> {}, latch::countDown);

        latch.await(5, TimeUnit.SECONDS);

        Assertions.assertThat(capture).containsExactly(1L);
        Assertions.assertThat(requested).containsExactly(1L, Long.MAX_VALUE);
    }

    @Test
    public void shouldBeRequestedExactlyOneAndThenLongMaxValueConditional() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        ArrayList<Long> capture = new ArrayList<>();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribe(capture::add, __ -> {}, latch::countDown);

        latch.await(5, TimeUnit.SECONDS);

        Assertions.assertThat(capture).containsExactly(1L);
        Assertions.assertThat(requested).containsExactly(1L, Long.MAX_VALUE);
    }

    @Test
    public void shouldReturnCorrectContextOnEmptySource() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        Flux<Long> switchTransformed = Flux.<Long>empty()
                .switchOnFirst((f, innerFlux) -> {
                    first[0] = f;
                    innerFlux.subscribe();

                    return Flux.<Long>empty();
                })
                .subscriberContext(Context.of("a", "c"))
                .subscriberContext(Context.of("c", "d"));

        StepVerifier.create(switchTransformed, 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .expectAccessibleContext()
                    .contains("a", "c")
                    .contains("c", "d")
                    .then()
                    .expectComplete()
                    .verify();

        Assertions.assertThat(first).containsExactly(Signal.complete(Context.of("a", "c").put("c", "d")));
    }

    @Test
    public void shouldReturnCorrectContextIfLoosingChain() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        Flux<Long> switchTransformed = Flux.<Long>empty()
                .switchOnFirst((f, innerFlux) -> {
                    first[0] = f;
                    return innerFlux;
                })
                .subscriberContext(Context.of("a", "c"))
                .subscriberContext(Context.of("c", "d"));

        StepVerifier.create(switchTransformed, 0)
                .expectSubscription()
                .thenRequest(1)
                .expectAccessibleContext()
                .contains("a", "c")
                .contains("c", "d")
                .then()
                .expectComplete()
                .verify();

        Assertions.assertThat(first).containsExactly(Signal.complete(Context.of("a", "c").put("c", "d")));
    }

    @Test
    public void shouldNotFailOnIncorrectPublisherBehavior() {
        TestPublisher<Long> publisher =
                TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
        Flux<Long> switchTransformed = publisher.flux()
                                                .switchOnFirst((first, innerFlux) -> innerFlux.subscriberContext(Context.of("a", "b")));

        StepVerifier.create(new Flux<Long>() {
            @Override
            public void subscribe(CoreSubscriber<? super Long> actual) {
                switchTransformed.subscribe(actual);
                publisher.next(1L);
            }
        }, 0)
                    .thenRequest(1)
                    .expectNext(1L)
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext(2L)
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .expectError()
                    .verifyThenAssertThat()
                    .hasDroppedErrors(3)
                    .tookLessThan(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    // Since context is immutable, with switchOnFirst it should not be mutable as well. Upstream should observe downstream
    // Inner should be able to access downstreamContext but should not modify upstream context after the first element
    public void shouldNotBeAbleToAccessUpstreamContext() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst(
                                                      (first, innerFlux) -> innerFlux.map(String::valueOf)
                                                                                     .subscriberContext(Context.of("a", "b"))
                                                  )
                                                  .subscriberContext(Context.of("a", "c"))
                                                  .subscriberContext(Context.of("c", "d"));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext("2")
                    .expectAccessibleContext()
                    .contains("a", "c")
                    .contains("c", "d")
                    .then()
                    .then(publisher::complete)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldNotHangWhenOneElementUpstream() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) ->
                                                      innerFlux.map(String::valueOf)
                                                               .subscriberContext(Context.of("a", "b"))
                                                  )
                                                  .subscriberContext(Context.of("a", "c"))
                                                  .subscriberContext(Context.of("c", "d"));

        publisher.next(1L);
        publisher.complete();

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void backpressureTest() {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.flux()
                                                  .doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext("2")
                    .then(publisher::complete)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();

        Assertions.assertThat(requested.get()).isEqualTo(2L);
    }

    @Test
    public void backpressureConditionalTest() {
        Flux<Integer> publisher = Flux.range(0, 10000);
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                                                  .filter(e -> false);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        Assertions.assertThat(requested.get()).isEqualTo(2L);

    }

    @Test
    public void backpressureHiddenConditionalTest() {
        Flux<Integer> publisher = Flux.range(0, 10000);
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf)
                                                                                                .hide())
                                                  .filter(e -> false);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        Assertions.assertThat(requested.get()).isEqualTo(10001L);
    }

    @Test
    public void backpressureDrawbackOnConditionalInTransformTest() {
        Flux<Integer> publisher = Flux.range(0, 10000);
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux
                                                          .map(String::valueOf)
                                                          .filter(e -> false));

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        Assertions.assertThat(requested.get()).isEqualTo(10001L);
    }

    @Test
    public void shouldErrorOnOverflowTest() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .then(() -> publisher.next(2L))
                    .expectErrorSatisfies(t -> Assertions
                        .assertThat(t)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("Can't deliver value due to lack of requests")
                    )
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldPropagateonCompleteCorrectly() {
        Flux<String> switchTransformed = Flux.empty()
                                             .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        StepVerifier.create(switchTransformed)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldPropagateOnCompleteWithMergedElementsCorrectly() {
        Flux<String> switchTransformed = Flux.empty()
                                             .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf)
                                                                                           .mergeWith(Flux.just("1", "2", "3")));

        StepVerifier.create(switchTransformed)
                    .expectNext("1", "2", "3")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldPropagateErrorCorrectly() {
        Flux<String> switchTransformed = Flux.error(new RuntimeException("hello"))
                                             .transform(flux -> new FluxSwitchOnFirst<>(
                                                     flux,
                                                     (first, innerFlux) -> innerFlux.map(
                                                             String::valueOf), true));

        StepVerifier.create(switchTransformed)
                    .expectErrorMessage("hello")
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly() throws InterruptedException {
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                .doOnCancel(latch2::countDown)
                .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                .doOnCancel(() -> {
                    try {
                        latch1.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })
                .cancelOn(Schedulers.elastic());

        publisher.next(1);

        StepVerifier stepVerifier = StepVerifier.create(switchTransformed, 0)
                .thenCancel()
                .verifyLater();

        latch1.countDown();
        stepVerifier.verify(Duration.ofSeconds(10));

        Assertions.assertThat(latch2.await(1, TimeUnit.SECONDS)).isTrue();

        Instant endTime = Instant.now().plusSeconds(5);
        while (!publisher.wasCancelled()) {
            if (endTime.isBefore(Instant.now())) {
                break;
            }
        }
        publisher.assertCancelled();
        publisher.assertWasRequested();
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly2() {
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) ->
                                                      innerFlux
                                                          .map(String::valueOf)
                                                          .take(1)
                                                  );

        publisher.next(1);
        publisher.next(2);
        publisher.next(3);
        publisher.next(4);

        StepVerifier.create(switchTransformed, 1)
                    .expectNext("1")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly3() {
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) ->
                                                          innerFlux
                                                                  .map(String::valueOf)
                                                  )
                                                  .take(1);

        publisher.next(1);
        publisher.next(2);
        publisher.next(3);
        publisher.next(4);

        StepVerifier.create(switchTransformed, 1)
                    .expectNext("1")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();
    }

    @Test
    public void shouldBeAbleToCatchDiscardedElement() {
        TestPublisher<Integer> publisher = TestPublisher.create();
        Integer[] discarded = new Integer[1];
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                                                  .doOnDiscard(Integer.class, e -> discarded[0] = e);

        StepVerifier.create(switchTransformed, 0)
                    .expectSubscription()
                    .then(() -> publisher.next(1))
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();

        Assertions.assertThat(discarded).containsExactly(1);
    }

    @Test
    public void shouldBeAbleToCatchDiscardedElementInCaseOfConditional() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        TestPublisher<Integer> publisher = TestPublisher.create();
        int[] discarded = new int[1];
        Flux<String> switchTransformed = publisher.flux()
                .doOnCancel(latch2::countDown)
                .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                .filter(t -> true)
                .doOnDiscard(Integer.class, e -> discarded[0] = e)
                .doOnCancel(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })
                .cancelOn(Schedulers.elastic());

        StepVerifier stepVerifier = StepVerifier.create(switchTransformed, 0)
                .expectSubscription()
                .then(() -> publisher.next(1))
                .thenCancel()
                .verifyLater();

        latch.countDown();
        stepVerifier.verify(Duration.ofSeconds(1));
        Assertions.assertThat(latch2.await(1, TimeUnit.SECONDS)).isTrue();

        Instant endTime = Instant.now().plusSeconds(5);
        while (discarded[0] == 0) {
            if (endTime.isBefore(Instant.now())) {
                break;
            }
        }

        publisher.assertCancelled();
        publisher.assertWasRequested();

        Assertions.assertThat(discarded).containsExactly(1);
    }

    @Test
    public void shouldBeAbleToCancelSubscription() throws InterruptedException {
        Flux<Long> publisher = Flux.just(1L);
        for (int j = 0; j < 100; j++) {
            ArrayList<Long> capturedElements = new ArrayList<>();
            ArrayList<Boolean> capturedCompletions = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                AtomicLong captureElement = new AtomicLong(0L);
                AtomicBoolean captureCompletion = new AtomicBoolean(false);
                AtomicLong requested = new AtomicLong();
                CountDownLatch latch = new CountDownLatch(1);
                Flux<Long> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                        .doOnCancel(latch::countDown)
                                                        .switchOnFirst((first, innerFlux) -> innerFlux)
                                                        .doOnComplete(latch::countDown);

                switchTransformed.subscribeWith(new LambdaSubscriber<>(
                    captureElement::set,
                    __ -> { },
                    () -> captureCompletion.set(true),
                    s -> ForkJoinPool.commonPool().execute(() ->
                        RaceTestUtils.race(s::cancel, () -> s.request(1), Schedulers.parallel())
                    )
                ));

                Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
                capturedElements.add(captureElement.get());
                capturedCompletions.add(captureCompletion.get());
            }

            Assertions.assertThat(capturedElements).contains(0L);
            Assertions.assertThat(capturedCompletions).contains(false);
        }
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnNextDuringSwitching() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        Optional<?> expectedCause = Optional.of(1L);

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;
                                    throw new NullPointerException();
                                }))
                    .expectSubscription()
                    .expectError(NullPointerException.class)
                    .verifyThenAssertThat()
                    .hasOperatorErrorsSatisfying(c ->
                        Assertions.assertThat(c)
                                  .hasOnlyOneElementSatisfying(t -> {
                                      Assertions.assertThat(t.getT1()).containsInstanceOf(NullPointerException.class);
                                      Assertions.assertThat(t.getT2()).isEqualTo(expectedCause);
                                  })
                    );


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnErrorDuringSwitching() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        NullPointerException npe = new NullPointerException();
        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;
                                    throw npe;
                                }))
                    .expectSubscription()
                    .verifyError(NullPointerException.class);


        Assertions.assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnCompleteDuringSwitching() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.<Long>empty()
                            .switchOnFirst((s, f) -> {
                                first[0] = s;
                                throw new NullPointerException();
                            })
                    )
                    .expectSubscription()
                    .expectError(NullPointerException.class)
                    .verifyThenAssertThat()
                    .hasOperatorErrorMatching(t -> {
                        Assertions.assertThat(t)
                                  .isInstanceOf(NullPointerException.class);
                        return true;
                    });


        Assertions.assertThat(first).containsExactly(Signal.complete());
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnNextDuringSwitchingConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Integer>[] first = new Signal[1];
        Optional<?> expectedCause = Optional.of(1);

        StepVerifier
            .create(
                Flux.range(1, 100)
                    .switchOnFirst((s, f) -> {
                        first[0] = s;
                        throw new NullPointerException();
                    })
                    .filter(__ -> true)
            )
            .expectSubscription()
            .expectError(NullPointerException.class)
            .verifyThenAssertThat()
            .hasOperatorErrorsSatisfying(c ->
                Assertions.assertThat(c)
                        .hasOnlyOneElementSatisfying(t -> {
                            Assertions.assertThat(t.getT1()).containsInstanceOf(NullPointerException.class);
                            Assertions.assertThat(t.getT2()).isEqualTo(expectedCause);
                        })
            );


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnErrorDuringSwitchingConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        NullPointerException npe = new NullPointerException();
        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                .switchOnFirst((s, f) -> {
                    first[0] = s;
                    throw npe;
                }).filter(__ -> true))
                .expectSubscription()
                .verifyError(NullPointerException.class);


        Assertions.assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnCompleteDuringSwitchingConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.<Long>empty()
                .switchOnFirst((s, f) -> {
                    first[0] = s;
                    throw new NullPointerException();
                }).filter(__ -> true)
        )
                .expectSubscription()
                .expectError(NullPointerException.class)
                .verifyThenAssertThat()
                .hasOperatorErrorMatching(t -> {
                    Assertions.assertThat(t)
                            .isInstanceOf(NullPointerException.class);
                    return true;
                });


        Assertions.assertThat(first).containsExactly(Signal.complete());
    }

    @Test
    public void sourceSubscribedOnce() {
        AtomicInteger subCount = new AtomicInteger();
        Flux<Integer> source = Flux.range(1, 10)
                                   .hide()
                                   .doOnSubscribe(subscription -> subCount.incrementAndGet());

        StepVerifier.create(source.switchOnFirst((s, f) -> f.filter(v -> v % 2 == s.get())))
                    .expectNext(1, 3, 5, 7, 9)
                    .verifyComplete();

        Assertions.assertThat(subCount).hasValue(1);
    }

    @Test
    public void checkHotSource() {
        ReplayProcessor<Long> processor = ReplayProcessor.create(1);

        processor.onNext(1L);
        processor.onNext(2L);
        processor.onNext(3L);


        StepVerifier.create(processor.switchOnFirst((s, f) -> f.filter(v -> v % s.get() == 0)))
                    .expectNext(3L)
                    .then(() -> {
                        processor.onNext(4L);
                        processor.onNext(5L);
                        processor.onNext(6L);
                        processor.onNext(7L);
                        processor.onNext(8L);
                        processor.onNext(9L);
                        processor.onComplete();
                    })
                    .expectNext(6L, 9L)
                    .verifyComplete();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherComplete() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.empty()))
                    .expectSubscription()
                    .verifyComplete();

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldNotCancelSourceOnUnrelatedPublisherComplete() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.empty(), false))
                .expectSubscription()
                .verifyComplete();

        Assertions.assertThat(testPublisher.isCancelled()).isFalse();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherError() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                    .expectSubscription()
                    .verifyErrorSatisfies(t ->
                        Assertions.assertThat(t)
                                  .hasMessage("test")
                                  .isExactlyInstanceOf(RuntimeException.class)
                    );

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCancel() {
        TestPublisher<Long> testPublisher = TestPublisher.create();

        StepVerifier.create(testPublisher.flux().switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                    .expectSubscription()
                    .thenCancel()
                    .verify();

        Assertions.assertThat(testPublisher.wasCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCompleteConditional() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.empty().delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        FluxPublish.PubSubInner<Long>[] subs = testPublisher.subscribers;
                        Assertions.assertThat(subs).hasSize(1);
                        Assertions.assertThat(subs[0])
                                  .extracting(psi -> psi.actual)
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .verifyComplete();

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldNotCancelSourceOnUnrelatedPublisherCompleteConditional() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.empty().delaySubscription(Duration.ofMillis(10)), false).filter(__ -> true))
                .then(() -> {
                    FluxPublish.PubSubInner<Long>[] subs = testPublisher.subscribers;
                    Assertions.assertThat(subs).hasSize(1);
                    Assertions.assertThat(subs[0])
                            .extracting(psi -> psi.actual)
                            .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                })
                .verifyComplete();

        Assertions.assertThat(testPublisher.isCancelled()).isFalse();
    }

    @Test
    public void shouldCancelInnerSubscriptionImmediatelyUpOnReceivingIfDownstreamIsAlreadyCancelledConditional() {
        VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
        TestPublisher<Long> testPublisher = TestPublisher.create();
        TestPublisher<Long> testPublisherInner = TestPublisher.create();

        try {
            StepVerifier
                .create(
                    testPublisher
                        .flux()
                        .switchOnFirst((s, f) ->
                            testPublisherInner
                                .flux()
                                .transform(Operators.lift((__, cs) -> new BaseSubscriber<Long>() {
                                    @Override
                                    protected void hookOnSubscribe(Subscription subscription) {
                                        Schedulers.parallel().schedule(() -> cs.onSubscribe(this), 1, TimeUnit.SECONDS);
                                    }
                                })),
                            false
                        )
                        .filter(__ -> true)
                )
                .expectSubscription()
                .then(() -> testPublisher.next(1L))
                .thenCancel()
                .verify();

            Assertions.assertThat(testPublisher.wasCancelled()).isTrue();
            Assertions.assertThat(testPublisherInner.wasCancelled()).isFalse();
            virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1000));
            Assertions.assertThat(testPublisherInner.wasCancelled()).isTrue();
        } finally {
            VirtualTimeScheduler.reset();
        }
    }

    @Test
    public void shouldCancelInnerSubscriptionImmediatelyUpOnReceivingIfDownstreamIsAlreadyCancelled() {
        VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
        TestPublisher<Long> testPublisher = TestPublisher.create();
        TestPublisher<Long> testPublisherInner = TestPublisher.create();

        try {
            StepVerifier
                    .create(
                            testPublisher
                                    .flux()
                                    .switchOnFirst((s, f) ->
                                                    testPublisherInner
                                                            .flux()
                                                            .transform(Operators.lift((__, cs) -> new BaseSubscriber<Long>() {
                                                                @Override
                                                                protected void hookOnSubscribe(Subscription subscription) {
                                                                    Schedulers.parallel().schedule(() -> cs.onSubscribe(this), 1, TimeUnit.SECONDS);
                                                                }
                                                            })),
                                            false
                                    )
                    )
                    .expectSubscription()
                    .then(() -> testPublisher.next(1L))
                    .thenCancel()
                    .verify();

            Assertions.assertThat(testPublisher.wasCancelled()).isTrue();
            Assertions.assertThat(testPublisherInner.wasCancelled()).isFalse();
            virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1000));
            Assertions.assertThat(testPublisherInner.wasCancelled()).isTrue();
        } finally {
            VirtualTimeScheduler.reset();
        }
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherErrorConditional() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test")).delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        FluxPublish.PubSubInner<Long>[] subs = testPublisher.subscribers;
                        Assertions.assertThat(subs).hasSize(1);
                        Assertions.assertThat(subs[0])
                                  .extracting(psi -> psi.actual)
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .verifyErrorSatisfies(t ->
                            Assertions.assertThat(t)
                                      .hasMessage("test")
                                      .isExactlyInstanceOf(RuntimeException.class)
                    );

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCancelConditional() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test")).delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        FluxPublish.PubSubInner<Long>[] subs = testPublisher.subscribers;
                        Assertions.assertThat(subs).hasSize(1);
                        Assertions.assertThat(subs[0])
                                  .extracting(psi -> psi.actual)
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .thenAwait(Duration.ofMillis(50))
                    .thenCancel()
                    .verify();

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelUpstreamBeforeFirst() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                .thenAwait(Duration.ofMillis(50))
                .thenCancel()
                .verify(Duration.ofSeconds(2));

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldContinueWorkingRegardlessTerminalOnDownstream() {
        TestPublisher<Long> testPublisher = TestPublisher.create();

        Flux<Long>[] intercepted = new Flux[1];

        StepVerifier.create(testPublisher.flux().switchOnFirst((s, f) -> {
            intercepted[0] = f;
            return Flux.just(2L);
        }, false))
                .expectSubscription()
                .then(() -> testPublisher.next(1L))
                .expectNext(2L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));

        Assertions.assertThat(testPublisher.wasCancelled()).isFalse();

        StepVerifier.create(intercepted[0])
                .expectSubscription()
                .expectNext(1L)
                .then(testPublisher::complete)
                .expectComplete()
                .verify(Duration.ofSeconds(1));
    }

    @Test
     public void shouldCancelSourceOnOnDownstreamTerminal() {
        TestPublisher<Long> testPublisher = TestPublisher.create();

        StepVerifier.create(testPublisher.flux().switchOnFirst((s, f) -> Flux.just(1L), true))
                .expectSubscription()
                .then(() -> testPublisher.next(1L))
                .expectNext(1L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));

        Assertions.assertThat(testPublisher.wasCancelled()).isTrue();
    }

    @Test
    public void racingTest() {
        for (int i = 0; i < 1000; i++) {
            CoreSubscriber[] subscribers = new CoreSubscriber[1];
            Subscription[] downstreamSubscriptions = new Subscription[1];
            Subscription[] innerSubscriptions = new Subscription[1];


            AtomicLong requested = new AtomicLong();

            Flux.just(2)
                    .doOnRequest(requested::addAndGet)
                    .switchOnFirst((s, f) -> new Flux<Integer>() {

                        @Override
                        public void subscribe(CoreSubscriber actual) {
                            subscribers[0] = actual;
                            f.subscribe(actual::onNext, actual::onError, actual::onComplete, (s) -> innerSubscriptions[0] = s);
                        }
                    })
                    .subscribe(__ -> {
                    }, __ -> {
                    }, () -> {
                    }, s -> downstreamSubscriptions[0] = s);

            CoreSubscriber subscriber = subscribers[0];
            Subscription downstreamSubscription = downstreamSubscriptions[0];
            Subscription innerSubscription = innerSubscriptions[0];
            downstreamSubscription.request(1);

            RaceTestUtils.race(() -> subscriber.onSubscribe(innerSubscription), () -> downstreamSubscription.request(1));

            Assertions.assertThat(requested.get()).isEqualTo(2);
        }
    }

    @Test
    public void racingConditionalTest() {
        for (int i = 0; i < 1000; i++) {
            CoreSubscriber[] subscribers = new CoreSubscriber[1];
            Subscription[] downstreamSubscriptions = new Subscription[1];
            Subscription[] innerSubscriptions = new Subscription[1];


            AtomicLong requested = new AtomicLong();

            Flux.just(2)
                .doOnRequest(requested::addAndGet)
                .switchOnFirst((s, f) -> new Flux<Integer>() {

                    @Override
                    public void subscribe(CoreSubscriber actual) {
                        subscribers[0] = actual;
                        f.subscribe(new Fuseable.ConditionalSubscriber<Integer>() {
                            @Override
                            public boolean tryOnNext(Integer integer) {
                                return ((Fuseable.ConditionalSubscriber)actual).tryOnNext(integer);
                            }

                            @Override
                            public void onSubscribe(Subscription s) {
                                innerSubscriptions[0] = s;
                            }

                            @Override
                            public void onNext(Integer integer) {
                                actual.onNext(integer);
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                actual.onError(throwable);
                            }

                            @Override
                            public void onComplete() {
                                actual.onComplete();
                            }
                        });
                    }
                })
                .filter(__ -> true)
                .subscribe(__ -> { }, __ -> { }, () -> { }, s -> downstreamSubscriptions[0] = s);

            CoreSubscriber subscriber = subscribers[0];
            Subscription downstreamSubscription = downstreamSubscriptions[0];
            Subscription innerSubscription = innerSubscriptions[0];
            downstreamSubscription.request(1);

            RaceTestUtils.race(() -> subscriber.onSubscribe(innerSubscription), () -> downstreamSubscription.request(1));

            Assertions.assertThat(requested.get()).isEqualTo(2);
        }
    }

    @Test
    public void unitRacingTest() {
        for (int i = 0; i < 10000; i++) {
            FluxSwitchOnFirst.AbstractSwitchOnFirstMain mockParent = Mockito.mock(FluxSwitchOnFirst.AbstractSwitchOnFirstMain.class);
            Mockito.doNothing().when(mockParent).request(Mockito.anyLong());
            Mockito.doNothing().when(mockParent).cancel();
            Subscription mockSubscription = Mockito.mock(Subscription.class);
            ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
            Mockito.doNothing().when(mockSubscription).request(longArgumentCaptor.capture());
            Mockito.doNothing().when(mockSubscription).cancel();
            AssertSubscriber<Object> subscriber = AssertSubscriber.create(0);
            FluxSwitchOnFirst.SwitchOnFirstControlSubscriber switchOnFirstControlSubscriber = new FluxSwitchOnFirst.SwitchOnFirstControlSubscriber<>(mockParent, subscriber, true);

            switchOnFirstControlSubscriber.request(10);
            RaceTestUtils.race(() -> switchOnFirstControlSubscriber.request(10), () -> switchOnFirstControlSubscriber.onSubscribe(mockSubscription), Schedulers.parallel());

            Assertions.assertThat(longArgumentCaptor.getAllValues().size()).isBetween(1, 2);
            if (longArgumentCaptor.getAllValues().size() == 1) {
                Assertions.assertThat(longArgumentCaptor.getValue()).isEqualTo(20L);
            } else if (longArgumentCaptor.getAllValues().size() == 2) {
                Assertions.assertThat(longArgumentCaptor.getAllValues()).containsExactly(10L, 10L);
            } else {
                Assertions.fail("Unexpected number of calls");
            }
        }
    }

    @Test
    public void onCompleteAndRequestRacingTest() {
        Long signal = 1L;
        Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain>[] factories = new Function[] {
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstMain((CoreSubscriber)assertSubscriber, (s, f) -> f, true),
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain((Fuseable.ConditionalSubscriber) assertSubscriber, (s, f) -> f, true)
        };
        for (Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain> factory : factories) {
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> assertSubscriber = AssertSubscriber.create(0);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(Operators.toConditionalSubscriber(assertSubscriber));

                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.onComplete(), () -> switchOnFirstMain.request(55));
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L)));
                assertSubscriber.assertSubscribed()
                        .awaitAndAssertNextValues(signal)
                        .assertComplete();
            }
        }
    }

    @Test
    public void onErrorAndRequestRacingTest() {
        Long signal = 1L;
        RuntimeException ex = new RuntimeException();
        Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain>[] factories = new Function[] {
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstMain((CoreSubscriber)assertSubscriber, (s, f) -> f, true),
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain((Fuseable.ConditionalSubscriber) assertSubscriber, (s, f) -> f, true)
        };
        for (Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain> factory : factories) {
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> assertSubscriber = AssertSubscriber.create(0);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(Operators.toConditionalSubscriber(assertSubscriber));

                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.onError(ex), () -> switchOnFirstMain.request(55));
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L)));
                assertSubscriber.assertSubscribed()
                        .awaitAndAssertNextValues(signal)
                        .assertErrorWith(t -> Assertions.assertThat(t).isEqualTo(ex));
            }
        }
    }

    @Test
    public void сancelAndRequestRacingWithOnCompleteAfterTest() {
        Long signal = 1L;
        Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain>[] factories = new Function[] {
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstMain((CoreSubscriber)assertSubscriber, (s, f) -> f, true),
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain((Fuseable.ConditionalSubscriber) assertSubscriber, (s, f) -> f, true)
        };
        for (Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain> factory : factories){
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                AtomicReference<Object> discarded = new AtomicReference<>();
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber assertSubscriber = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_DISCARD, (Consumer<Object>) o -> Assertions.assertThat(discarded.getAndSet(o)).isNull()), 0L);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(Operators.toConditionalSubscriber(assertSubscriber));

                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.cancel(), () -> switchOnFirstMain.request(55));
                switchOnFirstMain.onComplete();
                assertSubscriber.assertNotTerminated();
                Object discardedValue = discarded.get();
                if (discardedValue == null) {
                    assertSubscriber.awaitAndAssertNextValues(signal);
                } else {
                    Assertions.assertThat(discardedValue).isEqualTo(signal);
                }

                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L) || argument.equals(55L)));
            }
        }
    }

    @Test
    public void cancelAndRequestRacingOnErrorAfterTest() {
        Long signal = 1L;
        Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain>[] factories = new Function[] {
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstMain((CoreSubscriber)assertSubscriber, (s, f) -> f, true),
                (assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain((Fuseable.ConditionalSubscriber) assertSubscriber, (s, f) -> f, true)
        };
        for (Function<CoreSubscriber, FluxSwitchOnFirst.AbstractSwitchOnFirstMain> factory : factories) {
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                AtomicReference<Object> discarded = new AtomicReference<>();
                AtomicReference<Object> discardedError = new AtomicReference<>();
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> assertSubscriber = new AssertSubscriber<>(Context.of(
                        Hooks.KEY_ON_DISCARD, (Consumer<Object>) o -> Assertions.assertThat(discarded.getAndSet(o)).isNull(),
                        Hooks.KEY_ON_ERROR_DROPPED, (Consumer<Object>) o -> Assertions.assertThat(discardedError.getAndSet(o)).isNull()
                ), 0L);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(Operators.toConditionalSubscriber(assertSubscriber));

                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.cancel(), () -> switchOnFirstMain.request(55));
                switchOnFirstMain.onError(new NullPointerException());
                assertSubscriber.assertNotTerminated();
                Assertions.assertThat(discardedError.get()).isInstanceOf(NullPointerException.class);
                Object discardedValue = discarded.get();
                if (discardedValue == null) {
                    assertSubscriber.awaitAndAssertNextValues(signal);
                } else {
                    Assertions.assertThat(discardedValue).isEqualTo(signal);
                }

                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L) || argument.equals(55L)));
            }
        }
    }


    private static final class NoOpsScheduler implements Scheduler {

        static final NoOpsScheduler INSTANCE = new NoOpsScheduler();

        private NoOpsScheduler() {}

        @Override
        public Disposable schedule(Runnable task) {
            return Disposables.composite();
        }

        @Override
        public Worker createWorker() {
            return NoOpsWorker.INSTANCE;
        }

        static final class NoOpsWorker implements Worker {

            static final NoOpsWorker INSTANCE = new NoOpsWorker();

            @Override
            public Disposable schedule(Runnable task) {
                return Disposables.never();
            }

            @Override
            public void dispose() {

            }
        };
    }
}
