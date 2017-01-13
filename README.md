# CLJ-Jupyter-Player

This is designed to play a jupyter notebook.

http://jupyter.org/

This project provides a jar that will run all the cells against a kernel. This makes running a notebook a self contained operation suitable for submission as a batch job.

## Usage

```bash
java -jar IClojurePlayer.jar
Please supply kernel config path

Usage:
	Please supply kernel config path, notebook path and notebook output path
  -h, --help
  -k, --kernel-config-path KERNEL_CONFIG_PATH        File containing the kernel config
  -n, --notebook-path NOTEBOOK_PATH                  The notebook file
  -o, --notebook-output-path NOTEBOOK_OUTPUT_PATH    The output notebook file
  -d, --debug-connection-path DEBUG_CONNECTION_PATH  Use an existing connection file to connect to an already running kernel
  ```

  To run a note book against the python3 kernel:

  ```bash
  java -jar IClojurePlayer.jar -k tmp/kernels/python3/kernel.json -n tmp/notebooks/02.ipynb -o tmp/notebooks_output/02.ipynb
  ```

## Inspirations

Based on https://github.com/achesnais/clj-jupyter

## License

Copyright © 2017 Stephen Clayton

Distributed under the Eclipse Public License version 1.0.
