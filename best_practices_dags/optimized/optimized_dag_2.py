# It's ok to import modules that are not expensive to load at top-level of a DAG file
import random
import pendulum

# Expensive imports should be avoided as top level imports, because DAG files are parsed frequently, resulting in top-level code being executed.
#
# import pandas
# import torch
# import tensorflow
#


@task()
def do_stuff_with_pandas_and_torch():
    import pandas
    import torch
    # do some operations using pandas and torch
    pass


@task()
def do_stuff_with_tensorflow():
    import tensorflow
    # do some operations using tensorflow
    pass
