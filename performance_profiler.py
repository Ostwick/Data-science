import time
import tracemalloc
import logging
from functools import wraps

# Configure logging
logging.basicConfig(filename="performance_profiler.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def performance_profiler(func):
    """Decorator to measure execution time and memory usage of a function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        tracemalloc.start()
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        logging.info(f"Function '{func.__name__}' executed in {end_time - start_time:.6f} seconds.")
        logging.info(f"Memory usage: Current = {current / 10**6:.6f} MB; Peak = {peak / 10**6:.6f} MB")
        
        return result
    return wrapper

@performance_profiler
def example_function(n):
    """Example function that simulates workload."""
    total = sum(range(n))
    time.sleep(1)  # Simulating delay
    return total

if __name__ == "__main__":
    result = example_function(1000000)
    print("Execution completed. Check logs for details.")
