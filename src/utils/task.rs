use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Idle,
    Running,
    Completed,
}

/// Thread-safe handle for a task that runs at most once at a time.
#[derive(Debug, Clone)]
pub struct TaskHandle<T> {
    inner: Arc<Mutex<TaskState<T>>>,
}

#[derive(Debug)]
enum TaskState<T> {
    Idle,
    Running(oneshot::Receiver<T>),
    Completed(T),
}

impl<T> Default for TaskHandle<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> TaskHandle<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TaskState::Idle)),
        }
    }

    /// Returns the current status, polling for completion if running.
    pub async fn status(&self) -> TaskStatus {
        let mut state = self.inner.lock().await;
        try_complete(&mut state);
        match &*state {
            TaskState::Idle => TaskStatus::Idle,
            TaskState::Running(_) => TaskStatus::Running,
            TaskState::Completed(_) => TaskStatus::Completed,
        }
    }

    /// Attempts to start a new task.
    ///
    /// Returns `Some(sender)` if started successfully, `None` if already running.
    pub async fn try_start(&self) -> Option<oneshot::Sender<T>> {
        let mut state = self.inner.lock().await;
        try_complete(&mut state);

        if matches!(*state, TaskState::Running(_)) {
            return None;
        }

        let (tx, rx) = oneshot::channel();
        *state = TaskState::Running(rx);
        Some(tx)
    }

    /// Takes the completed result, resetting to Idle.
    pub async fn take_result(&self) -> Option<T> {
        let mut state = self.inner.lock().await;
        try_complete(&mut state);

        match std::mem::replace(&mut *state, TaskState::Idle) {
            TaskState::Completed(result) => Some(result),
            other @ TaskState::Idle | other @ TaskState::Running(_) => {
                *state = other;
                None
            }
        }
    }

    /// Waits for the running task to complete and returns the result.
    ///
    /// Returns `None` if idle or if the task was cancelled.
    pub async fn wait(&self) -> Option<T> {
        // Take the receiver out while holding the lock briefly
        let rx = {
            let mut state = self.inner.lock().await;
            match std::mem::replace(&mut *state, TaskState::Idle) {
                TaskState::Running(rx) => {
                    *state = TaskState::Idle; // Temporarily idle while waiting
                    Some(rx)
                }
                TaskState::Completed(result) => return Some(result),
                TaskState::Idle => return None,
            }
        };

        // Wait without holding the lock
        let result = rx?.await.ok()?;

        // Store the result
        let mut state = self.inner.lock().await;
        *state = TaskState::Completed(result);

        self.take_result().await
    }

    /// Resets the handle, cancelling any running task.
    pub async fn reset(&self) {
        let mut state = self.inner.lock().await;
        *state = TaskState::Idle;
    }
}

fn try_complete<T>(state: &mut TaskState<T>) {
    let TaskState::Running(rx) = state else {
        return;
    };

    match rx.try_recv() {
        Ok(result) => *state = TaskState::Completed(result),
        Err(oneshot::error::TryRecvError::Closed) => *state = TaskState::Idle,
        Err(oneshot::error::TryRecvError::Empty) => {}
    }
}
