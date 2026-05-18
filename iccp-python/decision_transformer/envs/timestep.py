import collections
import enum

class TimeStep(
    collections.namedtuple('TimeStep',
                           ['step_type', 'reward', 'discount', 'observation'])):
  """Returned with every call to `step` and `reset` on an environment.

  A `TimeStep` contains the data emitted by an environment at each step of
  interaction. A `TimeStep` holds a `step_type`, an `observation` (typically a
  NumPy array or a dict or list of arrays), and an associated `reward` and
  `discount`.

  The first `TimeStep` in a sequence will have `StepType.FIRST`. The final
  `TimeStep` will have `StepType.LAST`. All other `TimeStep`s in a sequence will
  have `StepType.MID.

  Attributes:
    step_type: A `StepType` enum value.
    reward:  A scalar, NumPy array, nested dict, list or tuple of rewards; or
      `None` if `step_type` is `StepType.FIRST`, i.e. at the start of a
      sequence.
    discount: A scalar, NumPy array, nested dict, list or tuple of discount
      values in the range `[0, 1]`, or `None` if `step_type` is
      `StepType.FIRST`, i.e. at the start of a sequence.
    observation: A NumPy array, or a nested dict, list or tuple of arrays.
      Scalar values that can be cast to NumPy arrays (e.g. Python floats) are
      also valid in place of a scalar array.
  """
  __slots__ = ()

  def first(self):
    # type: () -> bool
    return self.step_type == StepType.FIRST

  def mid(self):
    # type: () -> bool
    return self.step_type == StepType.MID

  def last(self):
    # type: () -> bool
    return self.step_type == StepType.LAST


class StepType(enum.IntEnum):
  """Defines the status of a `TimeStep` within a sequence."""
  # Denotes the first `TimeStep` in a sequence.
  FIRST = 0
  # Denotes any `TimeStep` in a sequence that is not FIRST or LAST.
  MID = 1
  # Denotes the last `TimeStep` in a sequence.
  LAST = 2

  def first(self):
    # type: () -> bool
    return self is StepType.FIRST

  def mid(self):
    # type: () -> bool
    return self is StepType.MID

  def last(self):
    # type: () -> bool
    return self is StepType.LAST


def restart(observation):
  """Returns a `TimeStep` with `step_type` set to `StepType.FIRST`."""
  return TimeStep(StepType.FIRST, None, None, observation)


def transition(reward, observation, discount=1.0):
  """Returns a `TimeStep` with `step_type` set to `StepType.MID`."""
  return TimeStep(StepType.MID, reward, discount, observation)


def termination(reward, observation):
  """Returns a `TimeStep` with `step_type` set to `StepType.LAST`."""
  return TimeStep(StepType.LAST, reward, 0.0, observation)


def truncation(reward, observation, discount=1.0):
  """Returns a `TimeStep` with `step_type` set to `StepType.LAST`."""
  return TimeStep(StepType.LAST, reward, discount, observation)