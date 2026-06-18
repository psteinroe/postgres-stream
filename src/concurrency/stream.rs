use core::pin::Pin;
use core::task::{Context, Poll};
use etl::config::BatchConfig;
use etl::types::SizeHint;
use futures::{Future, Stream, ready};
use pin_project_lite::pin_project;
use std::time::Duration;
use sysinfo::System;

const MIN_BATCH_BYTES: usize = 64 * 1024;

fn replay_batch_budget_bytes(batch_config: &BatchConfig) -> usize {
    let mut system = System::new();
    system.refresh_memory();

    let total_memory_bytes = usize::try_from(system.total_memory()).unwrap_or(usize::MAX);
    let budget = ((total_memory_bytes as f64) * f64::from(batch_config.memory_budget_ratio)).round()
        as usize;

    budget.max(MIN_BATCH_BYTES)
}

// Implementation adapted from:
//  https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/stream_ext/chunks_timeout.rs.
pin_project! {
    /// A stream adapter that batches fallible items based on byte budget and timeouts.
    ///
    /// This stream collects items from the underlying stream into batches, emitting them when either:
    /// - The accumulated successful items reach the configured memory budget
    /// - A timeout occurs
    /// - An error item is observed
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct TimeoutBatchStream<B, E, S: Stream<Item = Result<B, E>>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<tokio::time::Sleep>,
        items: Vec<S::Item>,
        current_batch_bytes: usize,
        max_batch_bytes: usize,
        batch_config: BatchConfig,
        reset_timer: bool,
        inner_stream_ended: bool,
        stream_stopped: bool
    }
}

impl<B, E, S> TimeoutBatchStream<B, E, S>
where
    B: SizeHint,
    S: Stream<Item = Result<B, E>>,
{
    /// Creates a new [`TimeoutBatchStream`] with the given configuration.
    ///
    /// The stream will batch items according to the provided `batch_config`.
    pub fn wrap(stream: S, batch_config: BatchConfig) -> Self {
        let max_batch_bytes = replay_batch_budget_bytes(&batch_config);

        TimeoutBatchStream {
            stream,
            deadline: None,
            items: Vec::new(),
            current_batch_bytes: 0,
            max_batch_bytes,
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
            stream_stopped: false,
        }
    }
}

impl<B, E, S> Stream for TimeoutBatchStream<B, E, S>
where
    B: SizeHint,
    S: Stream<Item = Result<B, E>>,
{
    type Item = Vec<S::Item>;

    /// Polls the stream for the next batch of items using a complex state machine.
    ///
    /// This method implements a batching algorithm that balances throughput
    /// and latency by collecting items into batches based on both byte budget
    /// and time constraints. The polling state machine handles multiple
    /// concurrent conditions.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        // Fast path: if the inner stream has already ended, we're done.
        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            // Fast path: if we've been marked as stopped, terminate immediately.
            if *this.stream_stopped {
                return Poll::Ready(None);
            }

            // PRIORITY 1: Timer management
            // Reset the timeout timer when starting a new batch or after emitting a batch
            if *this.reset_timer {
                this.deadline
                    .set(Some(tokio::time::sleep(Duration::from_millis(
                        this.batch_config.max_fill_ms,
                    ))));
                *this.reset_timer = false;
            }

            // PRIORITY 2: Poll underlying stream for new items
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => {
                    // No more items available right now, check if we should emit due to timeout.
                    break;
                }
                Poll::Ready(Some(item)) => {
                    if let Ok(value) = &item {
                        *this.current_batch_bytes =
                            this.current_batch_bytes.saturating_add(value.size_hint());
                    }
                    let item_is_err = item.is_err();
                    this.items.push(item);

                    // BUDGET-BASED EMISSION: If the batch reached its byte budget,
                    // emit immediately to keep replay batches bounded.
                    if *this.current_batch_bytes >= *this.max_batch_bytes || item_is_err {
                        *this.reset_timer = true; // Schedule timer reset for next batch.
                        *this.current_batch_bytes = 0;
                        return Poll::Ready(Some(std::mem::take(this.items)));
                    }
                    // Continue loop to collect more items or check other conditions.
                }
                Poll::Ready(None) => {
                    // STREAM END: Underlying stream finished.
                    // Return final batch if we have items, otherwise signal completion.
                    let last = if this.items.is_empty() {
                        None // No final batch needed.
                    } else {
                        *this.reset_timer = true; // Clean up timer state.
                        *this.current_batch_bytes = 0;
                        Some(std::mem::take(this.items))
                    };

                    *this.inner_stream_ended = true; // Mark stream as permanently ended.

                    return Poll::Ready(last);
                }
            }
        }

        // PRIORITY 3: Time-based emission check
        // If we have items and the timeout has expired, emit the current batch.
        if !this.items.is_empty()
            && let Some(deadline) = this.deadline.as_pin_mut()
        {
            ready!(deadline.poll(cx));
            *this.reset_timer = true;
            *this.current_batch_bytes = 0;

            return Poll::Ready(Some(std::mem::take(this.items)));
        }

        // No conditions met for batch emission - wait for more items or timeout.
        Poll::Pending
    }
}

/// Result of polling a [`TimeoutStream`].
///
/// This enum indicates whether the inner stream produced a value or a
/// timeout occurred because no item arrived within the configured duration.
pub enum TimeoutStreamResult<T> {
    /// A value produced by the inner stream.
    Value(T),
    /// A timeout occurred before the inner stream yielded a new item.
    Timeout,
}

pin_project! {
    /// A stream adapter that yields timeout markers when idle.
    ///
    /// This wrapper polls the inner stream and returns either produced values
    /// or [`TimeoutStreamResult::Timeout`] when no value arrives within
    /// `max_batch_fill_duration`.
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct TimeoutStream<B, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<tokio::time::Sleep>,
        reset_timer: bool,
        max_batch_fill_duration: Duration,
    }
}

impl<B, S: Stream<Item = B>> TimeoutStream<B, S> {
    /// Wraps a stream to emit timeouts when idle.
    ///
    /// The returned stream yields [`TimeoutStreamResult::Value`] for items from
    /// the inner stream or [`TimeoutStreamResult::Timeout`] when the configured
    /// `max_batch_fill_duration` elapses without a new item.
    pub fn wrap(stream: S, max_batch_fill_duration: Duration) -> Self {
        Self {
            stream,
            deadline: None,
            reset_timer: true,
            max_batch_fill_duration,
        }
    }

    /// Returns a pinned mutable reference to the inner stream.
    ///
    /// Use this to interact with the wrapped stream when a mutable reference is required
    /// while preserving pinning guarantees.
    #[must_use]
    pub fn get_inner(self: Pin<&mut Self>) -> Pin<&mut S> {
        self.project().stream
    }

    /// Marks the timer to be reset in the next poll.
    ///
    /// This method should be called when you want to tell the stream to restart the timer in the
    /// next poll.
    pub fn mark_reset_timer(self: Pin<&mut Self>) {
        let this = self.project();
        *this.reset_timer = true;
    }
}

impl<B, S: Stream<Item = B>> Stream for TimeoutStream<B, S> {
    type Item = TimeoutStreamResult<B>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // If the timer should be reset, it means that we want to start counting down again.
        if *this.reset_timer {
            let sleep = tokio::time::sleep(*this.max_batch_fill_duration);
            this.deadline.set(Some(sleep));
            *this.reset_timer = false;
        }

        // Check if timeout has already expired.
        let timeout_expired = this
            .deadline
            .as_pin_mut()
            .map(|deadline| deadline.poll(cx).is_ready())
            .unwrap_or(false);

        match this.stream.poll_next(cx) {
            Poll::Ready(Some(value)) => Poll::Ready(Some(TimeoutStreamResult::Value(value))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending if timeout_expired => {
                *this.reset_timer = true;
                Poll::Ready(Some(TimeoutStreamResult::Timeout))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
