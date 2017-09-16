# CLJ-Jupyter-Player

This is designed to play a jupyter notebook.

http://jupyter.org/

This project provides a jar that will run all the cells against a kernel. This makes running a notebook a self contained operation suitable for submission as a batch job. There is also the option to substitute cells from another notebook. (e.g. parameters)

## Usage

```bash
java -jar IClojurePlayer.jar
Please supply kernel config path

Usage:
	Please supply kernel config path, notebook path and notebook output path
  -h, --help
  -k, --kernel-config-path KERNEL_CONFIG_PATH            File containing the kernel config
  -n, --notebook-path NOTEBOOK_PATH                      The notebook file
  -o, --notebook-output-path NOTEBOOK_OUTPUT_PATH        The output notebook file
  -d, --debug-connection-path DEBUG_CONNECTION_PATH      Use an existing connection file to connect to an already running kernel
  -p, --preload-notebook-path PRELOAD_NOTEBOOK_PATH      Cells from this notebook will be injected at the index specified with -u
  -u, --update-preload-at-index UPDATE_PRELOAD_AT_INDEX  Index in the list of cells where the preload notebook will be placed
  -e, --extra-logging
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
