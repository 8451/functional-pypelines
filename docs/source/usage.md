# Usage 

## Introduction

There are many tasks in programming, especially Data Science, that can be best modeled as a sequence of data 
transformations. In such situations, it is ideal to be able to chain a series of functions together, passing the output
of one as the input to the next. 

In Python, there is no such way to chain functions together. For example, this is how we might compose 3 functions using
Python out-of-the-box.
    
```python
def double(x):
    return 2 * x

def negate(x):
    return -x

def to_string(x):
    return str(x)


# Inline composition
to_string(negate(double(2))) == -4


# Define new function
def str_of_neg_dbl(x):
  return to_string(negate(double(x)))

str_of_neg_dbl(2) == '-4'


# Assign output on each call
x = 2
x = double(x)
x = negate(x)
x = to_string(x)

x == '-4'
```

But with Pipelines, we can compose the functions using ``>>`` into a new function that chains the steps together. All
we need to do is decorate our functions with the `Pipeline.step` decorator.

```python
from functional_pypelines import Pipeline


@Pipeline.step
def double(x):
    return 2 * x


@Pipeline.step
def negate(x):
    return -x


@Pipeline.step
def to_string(x):
    return str(x)


# Inline Composition
(double >> negate >> to_string)(2) == '-4'

# Define new function
str_of_neg_dbl = double >> negate >> to_string
str_of_neg_dbl(2) == -4

# Can still use the functions like normal
double(2) == 4
to_string(True) == 'True'
```

Using the decorator gives you a lot of flexibility, but you can also use pypelines with undecorated functions, you just
need to start the pipeline with a call to `Pipeline()` to kick it off, and wrap the whole thing in parentheses when 
passing the input data inline.

```python
from functional_pypelines import Pipeline


def double(x):
    return 2 * x


def negate(x):
    return -x


def to_string(x):
    return str(x)


# Inline Composition
(Pipeline() >> double >> negate >> to_string)(2) == '-4'

# Define new function
str_of_neg_dbl = Pipeline() >> double >> negate >> to_string
str_of_neg_dbl(2) == -4
```

## JSON Config API

In addition to letting you write more expressive code, Functional Pypelines also allows you to run a sequence of functions via a 
JSON config. For example, if our three functions `double`, `negate`, and `to_string` lived in a `functions.py` file,
we could accomplish the same task using the following config.

```json
{
  "PIPELINE": [
    "functions.double",
    "functions.negate",
    "functions.to_string"
  ],
  "DATA": 2
}
```

With this config we can either run this from the command line like so:

```bash
functional_pypelines -c conf.json
```

Or if we had the same config as a Python dictionary we can run from Python like so:

```python
import functional_pypelines

config = {
    "PIPEILNE": [
        "functions.double",
        "functions.negate",
        "functions.to_string"
    ],
    "DATA": 2
}

functional_pypelines.run(config) == '-4'
```


## Extending Pipeline

While Functional Pypelines is powerful out of the box, it may be a bit limiting to only write functions that pass one value around.
For more complex tasks, the `Pipeline` class can be subclassed to customize the behavior. The `Pipeline.step` decorator
can be overridden to allow for more complex functionality, such as passing multiple arguments to a function.