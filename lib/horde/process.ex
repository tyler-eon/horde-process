defmodule Horde.Process do
  @moduledoc """
  A module that uses `Horde.Process` is a `GenServer` process that is managed via Horde. Horde processes are started and supervised by a Horde Supervisor and registered with a Horde Registry. Many boilerplate functions providing interactions with Horde are automatically imported by this module when you `use Horde.Process`.

  Required options for `use Horde.Process` are as follows:

  - `:supervisor` - The name of the Horde Supervisor to use, e.g. `MyApp.HordeSupervisor`.
  - `:registry` - The name of the Horde Registry to use, e.g. `MyApp.HordeRegistry`.

  Additionally, all Horde Process modules *MUST* implement the following functions:

  - `process_id/1` which should return a unique process identifier. This is used for process lookup and registration.
  - `child_spec/1` returns the child spec used by the supervisor to start the process.

  The above functions are defined via `@callback` to ensure compiler feedback on the implementation of these functions.

  ## Horde Registry Notice

  When implementing a Horde Process, it is *CRITICAL* to use `{:continue, term}` in the `init/1` callback whenever non-trivial initialization is required. Horde CANNOT add the process to the registry until `init/1` completes. The longer it takes `init/1` to finish, the longer before the process can enter the registry and be used.

  This will have a knock-on effect of causing `fetch/1` to return `nil` even though `start/1` will return `:ignore`. To reduce the likelihood of this scenario, it's recommended to use `{:continue, term}` in `init/1` to ensure the process is started and registered as quickly as possible, then performing all non-trivial initialization in `handle_continue/2`.

  Even if you have the fastest `init/1` ever, there's still a possibility - within a distributed system - for Horde to be unable to fetch a process but also unable to start the process. This is because the local node ensures that you can't start a second process with the same unique process id, but the Horde registry is eventually consistent and so remote nodes won't know if a process is starting up elsewhere. And so this module implements "wait functions" to help retry the entire process a set number of times over a set period of time. It isn't guaranteed to always work, but it's configurable so that you can make it work best for your application.

  ## Example

  If you wanted to use this module to create a very simple Horde Process, you could do the following:

  ```elixir
  defmodule MyApp.User.Process do
    use Horde.Process, supervisor: MyApp.User.HordeSupervisor, registry: MyApp.User.HordeRegistry

    @impl Horde.Process
    def process_id(%{"user_id" => user_id}), do: user_id
    def process_id(%{user_id: user_id}), do: user_id
    def process_id(user_id) when is_binary(user_id), do: user_id

    @impl Horde.Process
    def child_spec(user_id) do
      %{
        id: user_id,
        start: {__MODULE__, :start_link, [user_id]},
        restart: :transient,
        shutdown: 10_000
      }
    end

    @impl GenServer
    # Set up the process state quickly and have `handle_continue/2` do the rest.
    def init(user_id) do
      Process.flag(:trap_exit, true)
      {:ok, user_id, {:continue, :init}}
    end

    @impl GenServer
    def handle_continue(:init, user_id) do
      {:ok, user} = MyApp.User.fetch(user_id)
      {:noreply, user}
    end
  end
  ```

  The supervisor and registry must be started appropriately by your application to be used by the Horde Process.

  We can see that `process_id/1` can convert a map or a binary string to a user id, and that `child_spec/1` takes in that user id to return a child process specification.

  We can also see the use of `{:continue, :init}` to ensure `init/1` completes as quickly as possible. Since we're loading the user from the database, we don't want to block the process from being registered with the Horde registry until that's done. Instead, we set the process state to `user_id` temporarily and pass along `{:continue, :init}` so that we know to take that user id and grab the full user schema from the database in `handle_continue/2`.

  You can then implement `handle_call/3` and `handle_cast/2` as you would with any other `GenServer` module. You may either invoke these directly or use the imported functions provided by `Horde.Process`, e.g. `MyApp.User.Process.call!/2`.
  """

  @typedoc """
  Represents a module that uses `Horde.Process`.
  """
  @type horde_process :: Module.t()

  @typedoc """
  Any term that will be passed to `process_id/1` to generate a unique process identifier.
  """
  @type ident :: term()

  @doc """
  Given an arbitrary argument, return a unique process identifier. This will, among other things, be used to register the process with a Horde Registry.
  """
  @callback process_id(term) :: term

  @doc """
  Given a unique process id, returns a child spec that will be used by the Horde Supervisor to start the process.

  When using the functions provided by `Horde.Process`, the process id given will come from `process_id/1`.
  """
  @callback child_spec(term) :: Supervisor.child_spec()


  @doc """
  When you `use Horde.Process` you must specify *at least* the following options:

  - `:supervisor` - The name of the Horde Supervisor to use, e.g. `MyApp.HordeSupervisor`.
  - `:registry` - The name of the Horde Registry to use, e.g. `MyApp.HordeRegistry`.

  And you may optionally specify:

  - `:wait_sleep` - The number of milliseconds to sleep between attempts to fetch a process from the registry. Defaults to `100`.
  - `:wait_max` - The maximum number of attempts to fetch a process from the registry. Defaults to `5`.

  In addition to generating functions that are tied to the given Horde Supervisor and Registry modules, this macro also performs  `use GenServer` and `@behavior Horde.Process`.

  ## Wait Options

  The `:wait_*` options are used by a generated function `wait_for_init/2`. They tell the function:

  1. How long to wait between attempts to fetch a process from the registry.
  2. The maximum number of attempts to fetch a process from the registry.

  If the maximum number of attempts has been reached without fetching a PID, `wait_for_init/2` will return `{:error, :not_found}`. Since the registry is eventually consistent, it's possible to fetch a process that can't be started because it's already in the middle of starting, but also which has not yet been added to the Horde Registry. This is a common scenario when using `start/1` and/or `fetch/1` at the same time from different nodes in a distributed system.

  In systems with a high degree of concurrency per unique process, there is a greater chance that some amount of waiting will be necessary. The default values are set to be conservative, but you may need to adjust them based on your application's needs. For example, if you have a high degree of concurrency then it might make sense to increase `:wait_max` to a higher number. If you can afford to have processes wait longer before timing out, you might increase `:wait_sleep` to a higher number. If you prefer to "fail fast" when processes can't start (maybe you have an external message queue that can replay messages safely), you might decrease `:wait_max` to a lower number. You will likely need to just play around with these values to find what works best for your application.
  """
  defmacro __using__(opts) do
    supervisor = Keyword.fetch!(opts, :supervisor)
    registry = Keyword.fetch!(opts, :registry)
    wait_sleep = Keyword.get(opts, :wait_sleep, 100)
    wait_max = Keyword.get(opts, :wait_max, 5)

    quote do
      use GenServer

      @behaviour Horde.Process

      @doc false
      def start_link(term), do: Horde.Process.start_link(__MODULE__, term, via_tuple(term))

      @doc """
      Starts a new Horde Process. This is *not* the same as `GenServer.start`, but rather an entry point to Horde's dynamic supervisor.
      """
      @spec start(term()) :: {:ok, pid()} | {:error, term()} | :ignore
      def start(term) do
        spec =
          term
          |> process_id()
          |> child_spec()

        Horde.DynamicSupervisor.start_child(unquote(supervisor), spec)
      end

      @doc """
      Helper function to generate a `{:via, _, _}` tuple that can be used to register processes during `GenServer.start` or `GenServer.start_link`.

      Uses `process_id/1` to extract the unique process identifier from the sole argument of this function, `term`.
      """
      @spec via_tuple(term()) :: {:via, atom(), {atom(), term()}}
      def via_tuple(term), do: {:via, Horde.Registry, {unquote(registry), process_id(term)}}

      @doc """
      Attempts to fetch the PID of an existing process. If no such process exists in the registry (i.e. `fetch/1` returns nil), an attempt will be made to start a new Horde Process and return that PID.

      The maximum amount of time this function can take to return is determined by the "Wait Options" specified via the `__using__/1` macro. If no existing process is in the registry and no new process can be started within the configured time, this function will return `{:error, :not_found}`.

      *Note*: It's possible for some *other* error to be returned as well. This function only matches on `{:ok, pid}` and `:ignore`; any other value will be returned to the caller.
      """
      @spec get(term()) :: {:ok, pid()} | {:error, term()}
      def get(term) do
        case fetch(term) do
          nil ->
            case start(term) do
              :ignore ->
                Horde.Process.wait_for_init(unquote(registry), process_id(term), 1, unquote(wait_sleep), unquote(wait_max))

              res ->
                res
            end

          pid ->
            {:ok, pid}
        end
      end

      @doc false
      def fetch(term), do: Horde.Process.fetch(unquote(registry), process_id(term))

      @doc false
      def call(term, message), do: Horde.Process.call(__MODULE__, term, message)

      @doc false
      def call!(term, message), do: Horde.Process.call!(__MODULE__, term, message)

      @doc false
      def cast(term, message), do: Horde.Process.cast(__MODULE__, term, message)

      @doc false
      def cast!(term, message), do: Horde.Process.cast!(__MODULE__, term, message)

      defoverridable [start_link: 1]
    end
  end

  @doc """
  Starts and monitors a new Horde Process (`module`) via `GenServer.start_link/3`. If `name` is already in use, this will return `:ignore`.
  """
  def start_link(module, term, name) do
    case GenServer.start_link(module, term, name: name) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, _}} -> :ignore
      error -> error
    end
  end

  @doc """
  Returns the PID from a Horde Registry using the unique process id. If no such process is registered, this will return `nil`.
  """
  @spec fetch(atom(), term()) :: pid() | nil
  def fetch(registry, id) do
    case Horde.Registry.lookup(registry, id) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @doc """
  A simple recursive function that will attempt to fetch a PID from a Horde Registry, waiting a short period of time between attempts.

  It's possible to fail to fetch a PID as well as get an `:ignore` response if the child process has not finished initialization yet. To understand why, we need to look at how Horde starts and registers processes.

  1. We use the `process_id/1` callback to determine a unique identifier for the process.
  2. Horde attempts to look up that identifier in the registry. If not found, it moves on to the next step.
  3. Horde uses DeltaCRDT to determine which node that identifier is associated with. The same identifier will always be associated with the same node in the same cluster topology.
  4. Horde sends a `start_link/1` RPC payload to the remote node to start a new process.
  5. Once the `init/1` function has completed, Horde receives the PID and adds it to the registry.
  6. The local registry is updated immediately but the distributed registry is eventually consistent.

  Steps 5 and 6 are where the problem lies. If `init/1` hasn't completed, or the registry of a remote node has not been made consistent with the recent state of processes, then the local node will return `nil` on a fetch PID call but will also return `:ignore` on a start process call. There are only two ways to handle this scenario:

  1. Give up. Totally reasonable if you don't *need* to have access to the process immediately.
  2. Wait a bit and try again, possibly a few times before giving up. But then there's a danger of causing a message inbox to fill up while it waits for a remote process to start.

  If the first solution works, simply pass zero for the `wait_max` argument and any `attempt` value greater than zero will cause `{:error, :not_found}` to be returned. If the second solution works, pass a greater-than-zero integer value for `wait_max` and a non-negative integer value for `wait_sleep`. The maximum amount of time this function might take to return to the caller is approximately `wait_max * wait_sleep` milliseconds.

  This is why it is recommended to use `{:continue, :init, state}` in the `init/1` callback and move all non-trivial initialization to `handle_continue/2`. This way, the process can be started and registered as quickly as possible, reducing how long we might sit in `wait_for_init` before getting a pid or returning an error.
  """
  def wait_for_init(registry, id, attempt, wait_sleep, wait_max) when attempt <= wait_max do
    case fetch(registry, id) do
      nil ->
        Process.sleep(wait_sleep)
        wait_for_init(registry, id, attempt + 1, wait_sleep, wait_max)

      pid ->
        {:ok, pid}
    end
  end

  def wait_for_init(_, _), do: {:error, :not_found}

  @doc """
  Attempts to invoke `GenServer.call/2` on a Horde Process.

  If no registered process exists, this will return `{:error, :not_found}`. If the process does exist, `{:ok, response}` is returned, where `response` is the response from `GenServer.call/2`.

  *Note*: Because of how this function wraps the response of `GenServer.call/2`, it's possible to get a result tuple that looks like the following:

      {:ok, {:error, err}}

  Because the first element of the tuple is `:ok`, that means a valid process was found and received the message. The second element, `{:error, :invalid}`, is the response from the process.
  """
  @spec call(horde_process(), ident(), term(), timeout()) :: {:ok, term()} | {:error, term()}
  def call(module, term, message, timeout \\ 5000) do
    case module.fetch(term) do
      nil ->
        {:error, :not_found}

      pid ->
        {:ok, GenServer.call(pid, message, timeout)}
    end
  end

  @doc """
  Executes `GenServer.call/2` on a Horde Process, starting a new process if necessary.

  If the process is not running and cannot be started, this function will result in an exception.
  """
  @spec call(horde_process(), ident(), term(), timeout()) :: term()
  def call!(module, term, message, timeout \\ 5000) do
    {:ok, pid} = module.get(term)
    GenServer.call(pid, message, timeout)
  end

  @doc """
  Attempts to invoke `GenServer.cast/2` on a Horde Process.

  If no registered process exists, this will return `nil`. Otherwise, it will return `:ok`.
  """
  @spec cast(horde_process(), ident(), term()) :: :ok | nil
  def cast(module, term, message) do
    case module.fetch(term) do
      nil ->
        nil

      pid ->
        GenServer.cast(pid, message)
    end
  end

  @doc """
  Executes `GenServer.cast/2` on a Horde Process, starting a new process if necessary.

  If the process is not running and cannot be started, this function will result in an exception.
  """
  @spec cast!(horde_process(), ident(), term()) :: :ok
  def cast!(module, term, message) do
    {:ok, pid} = module.get(term)
    GenServer.cast(pid, message)
  end
end
