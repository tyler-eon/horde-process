defmodule Horde.ProcessTest do
  use ExUnit.Case
  doctest Horde.Process

  test "greets the world" do
    assert Horde.Process.hello() == :world
  end
end
