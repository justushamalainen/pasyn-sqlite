use pyo3::prelude::*;
use pyo3::exceptions::PyStopIteration;

/// A simple awaitable that returns "hello world" immediately.
///
/// This demonstrates the basic pattern for creating native Python awaitables in Rust.
/// The Python `await` mechanism works like this:
///
/// 1. Python calls `__await__()` on the object, which returns an iterator
/// 2. Python calls `__next__()` on the iterator repeatedly
/// 3. Each `__next__()` call either:
///    - Returns a value to yield (for cooperative multitasking with event loop)
///    - Raises `StopIteration(result)` to signal completion with a result
#[pyclass]
struct HelloAwaitable {
    completed: bool,
}

#[pymethods]
impl HelloAwaitable {
    #[new]
    fn new() -> Self {
        HelloAwaitable { completed: false }
    }

    /// Called when the object is awaited: `await obj`
    /// Returns self (the iterator)
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Iterator protocol - returns self
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Iterator next - either yields or raises StopIteration
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
        if slf.completed {
            // Already completed, raise StopIteration with None
            Err(PyStopIteration::new_err(Python::with_gil(|py| {
                py.None()
            })))
        } else {
            slf.completed = true;
            // Raise StopIteration with our result "hello world"
            // This is how a coroutine returns its value
            Err(PyStopIteration::new_err("hello world"))
        }
    }
}

/// A more advanced awaitable that demonstrates GIL release during blocking work.
/// This simulates doing work without holding the GIL.
#[pyclass]
struct BlockingAwaitable {
    completed: bool,
    result: Option<String>,
}

#[pymethods]
impl BlockingAwaitable {
    #[new]
    fn new() -> Self {
        BlockingAwaitable {
            completed: false,
            result: None,
        }
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if slf.completed {
            let result = slf.result.take().unwrap_or_default();
            Err(PyStopIteration::new_err(result))
        } else {
            // Release the GIL and do "blocking" work
            let result = py.allow_threads(|| {
                // Simulate some work without holding the GIL
                // In real code, this would be socket I/O
                std::thread::sleep(std::time::Duration::from_millis(10));
                "hello from blocking work".to_string()
            });

            slf.completed = true;
            slf.result = Some(result.clone());

            // Return the result via StopIteration
            Err(PyStopIteration::new_err(result))
        }
    }
}

/// An awaitable that properly integrates with asyncio event loop.
///
/// This pattern properly delegates to asyncio.Future's iterator.
/// When awaiting a Future, we must use its __await__ iterator, not yield it directly.
#[pyclass]
struct AsyncIOAwaitable {
    state: u8,  // 0 = initial, 1 = iterating future, 2 = done
    future_iter: Option<PyObject>,  // The iterator from Future.__await__()
}

#[pymethods]
impl AsyncIOAwaitable {
    #[new]
    fn new() -> Self {
        AsyncIOAwaitable {
            state: 0,
            future_iter: None,
        }
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// This demonstrates the full async pattern with proper Future delegation.
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        match slf.state {
            0 => {
                // First call - create a Future and get its iterator
                let asyncio = py.import_bound("asyncio")?;
                let loop_obj = asyncio.call_method0("get_running_loop")?;

                // Create a new Future
                let future = loop_obj.call_method0("create_future")?;

                // Schedule the result to be set (simulating async completion)
                let callback = py.import_bound("functools")?.call_method1(
                    "partial",
                    (
                        future.getattr("set_result")?,
                        "hello from asyncio",
                    ),
                )?;
                loop_obj.call_method1("call_soon", (callback,))?;

                // Get the Future's __await__ iterator - this is what we delegate to
                let future_iter = future.call_method0("__await__")?;
                slf.future_iter = Some(future_iter.clone().unbind());
                slf.state = 1;

                // Now call __next__ on the future's iterator
                Self::drive_future_iter(py, &future_iter)
            }
            1 => {
                // Continue iterating the future
                if let Some(ref future_iter) = slf.future_iter {
                    let iter_bound = future_iter.bind(py);
                    match Self::drive_future_iter(py, iter_bound) {
                        Ok(value) => Ok(value),
                        Err(e) if e.is_instance_of::<PyStopIteration>(py) => {
                            // Future completed - extract the result
                            slf.state = 2;
                            let stop_iter_obj = e.into_value(py);
                            let stop_iter = stop_iter_obj.bind(py);
                            let result = stop_iter.getattr("value")?;
                            let result_str: String = result.extract()?;
                            Err(PyStopIteration::new_err(result_str))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    Err(PyStopIteration::new_err(py.None()))
                }
            }
            _ => {
                Err(PyStopIteration::new_err(py.None()))
            }
        }
    }
}

impl AsyncIOAwaitable {
    /// Drive the future's iterator forward
    fn drive_future_iter(py: Python<'_>, iter: &Bound<'_, PyAny>) -> PyResult<Option<PyObject>> {
        let builtins = py.import_bound("builtins")?;
        let next_fn = builtins.getattr("next")?;

        match next_fn.call1((iter,)) {
            Ok(value) => Ok(Some(value.unbind())),
            Err(e) => Err(e),
        }
    }
}

/// Create a simple awaitable that returns "hello world" immediately
#[pyfunction]
fn hello_awaitable() -> HelloAwaitable {
    HelloAwaitable::new()
}

/// Create an awaitable that does blocking work with GIL released
#[pyfunction]
fn blocking_awaitable() -> BlockingAwaitable {
    BlockingAwaitable::new()
}

/// Create an awaitable that properly integrates with asyncio
#[pyfunction]
fn asyncio_awaitable() -> AsyncIOAwaitable {
    AsyncIOAwaitable::new()
}

#[pymodule]
fn pasyn_await_poc(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<HelloAwaitable>()?;
    m.add_class::<BlockingAwaitable>()?;
    m.add_class::<AsyncIOAwaitable>()?;
    m.add_function(wrap_pyfunction!(hello_awaitable, m)?)?;
    m.add_function(wrap_pyfunction!(blocking_awaitable, m)?)?;
    m.add_function(wrap_pyfunction!(asyncio_awaitable, m)?)?;
    Ok(())
}
