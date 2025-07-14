# Pybind11 C++ Example with Python

## Build and Run in Docker

1. **Build the Docker image:**
   ```sh
   docker build -t cpp_pybind_example .
   ```

2. **Run the container (with interactive shell):**
   ```sh
   docker run --rm -it -v $(pwd):/app cpp_pybind_example
   ```

3. **Inside the container, build the C++ extension:**
   ```sh
   cmake -S . -B build
   cmake --build build
   export PYTHONPATH=$PYTHONPATH:/app/build
   ```

4. **Test from Python:**
   ```sh
   python3 test_example.py
   ```

You should see:

```
Hello from C++!
``` 