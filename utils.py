import time

def timer(func):
    """A decorator that prints the execution time for the decorated function"""
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        print_color(f"[I]Function {func.__name__} from module {func.__module__} took {end_time - start_time:,.1f} seconds","cyan")
        return result
    return wrapper
