"""
Provide objects to manage results from concurrent operations using ThreadPoolExecutor.
"""
import multiprocessing
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, List, Set, Callable, Generator, Optional


class _AsyncCollection:
    def __init__(self, executor: ThreadPoolExecutor, concurrency: int=0):
        self.executor = executor
        self.concurrency = concurrency
        assert 0 < self.concurrency

    def __len__(self):
        raise NotImplementedError()

    def __bool__(self) -> bool:
        return bool(len(self))

    def put(self, func: Callable, *args, **kwargs):
        raise NotImplementedError()

    def get(self) -> Any:
        raise NotImplementedError()

    def consume(self) -> Generator[Any, None, None]:
        raise NotImplementedError()

    def consume_finished(self) -> Generator[Any, None, None]:
        raise NotImplementedError()

    def _running(self) -> Set[Future]:
        futures = getattr(self, "_futures", list())
        return {f for f in futures if not f.done()}

    def abort(self):
        futures = getattr(self, "_futures", list())
        for f in futures:
            f.cancel()
        for _ in as_completed(futures):
            pass

    def __del__(self):
        self.abort()

class AsyncSet(_AsyncCollection):
    """
    Unordered collection providing results of concurrent operations. Up to `concurrency` operations are executed in
    parallel.
    """
    def __init__(self, executor: ThreadPoolExecutor, concurrency: int=multiprocessing.cpu_count()):
        super().__init__(executor, concurrency)
        self._futures: Set[Future] = set()

    def __len__(self):
        return len(self._futures)

    def put(self, func: Callable, *args, **kwargs):
        """
        Submit a new operation to the executor. Block until the number of in-progress operations becomes less than or
        equal to `concurrency`.
        """
        running = len(self._running())
        if running >= self.concurrency:
            self._wait(running - self.concurrency)
        f = self.executor.submit(func, *args, **kwargs)
        self._futures.add(f)

    def get(self) -> Any:
        """
        Remove and return any result. Wait until a result is available.
        """
        for r in self.consume():
            return r

    def consume(self) -> Any:
        """
        Remove and yield all results as available, waiting as needed.
        """
        for f in as_completed(self._futures):
            self._futures.remove(f)
            yield f.result()

    def consume_finished(self) -> Generator[Any, None, None]:
        """
        Remove and yield results from completed operations.
        """
        for f in self._futures.copy():
            if f.done():
                self._futures.remove(f)
                yield f.result()

    def _wait(self, count: int=-1):
        for i, f in enumerate(as_completed(self._running())):
            f.result()
            if i == count:
                break

class AsyncQueue(_AsyncCollection):
    """
    FIFO queue providing results of concurrent operations in the order they were submitted. Up to `concurrency`
    operations are executed in parallel. New operations are executed concurrently as available results are consumed.
    """
    def __init__(self, executor: ThreadPoolExecutor, concurrency: int=multiprocessing.cpu_count()):
        super().__init__(executor, concurrency)
        self._futures: List[Future] = list()  # type: ignore
        self._to_be_submitted: List[Any] = list()

    def __len__(self):
        return len(self._to_be_submitted) + len(self._futures)

    def _submit_futures(self):
        while len(self._futures) < self.concurrency:
            try:
                func, args, kwargs = self._to_be_submitted.pop(0)
            except IndexError:
                return
            f = self.executor.submit(func, *args, **kwargs)
            self._futures.append(f)

    def put(self, func: Callable, *args, **kwargs):
        """
        Schedule a new operation for execution.
        """
        self._to_be_submitted.append((func, args, kwargs))
        self._submit_futures()

    def get(self) -> Any:
        """
        Remove and return the next result in order. Wait until available.
        """
        res = None
        for f in as_completed(self._futures[:1]):
            res = self._futures.pop(0).result()
        self._submit_futures()
        return res

    def consume(self) -> Generator[Any, None, None]:
        """
        Remove and yield all results in order, waiting as needed.
        """
        while self._to_be_submitted or self._futures:
            yield self.get()
