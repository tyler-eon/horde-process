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

  The `:wait_*` options are used by `wait_for_init/2` to determine how long to wait for a process to be registered before giving up and returning `{:error, :not_found}`. Since the registry is eventually consistent, it's possible to fetch a process that can't be started because it's already in the middle of starting, but also which has not yet been added to (or replicated by) the Horde Registry. This is a common scenario when using `start/1` and/or `fetch/1` at the same time from different nodes in a distributed system.

  The maximum amount of time that `wait_for_init/2` will wait before failing is `wait_sleep * wait_max` milliseconds, which defaults to 500 milliseconds. If your application is creating and registering processes quickly, you can try decreasing `:wait_sleep` while increasing `:wait_max`; this means retries will happen more frequently. If, however, you application is creating and registering processes slowly, you can try increasing `:wait_sleep` to avoid unnecessary retries. If you prefer to fail quickly when a process is not registered, you can decrease `:wait_max` to a lower number.
  """
  defmacro __using__(opts) do
    supervisor = Keyword.fetch!(opts, :supervisor)
    registry = Keyword.fetch!(opts, :registry)
    wait_sleep = Keyword.get(opts, :wait_sleep, 100)
    wait_max = Keyword.get(opts, :wait_max, 5)

    quote do
      import Horde.Process

      @behaviour Horde.Process

      @doc """
      Starts and monitors a new Horde Process via `GenServer.start_link/3`. If the unique process id extracted from `term` is already in use, this will return `:ignore`.

      The `term` argument will be passed as-is to `init/1`. It will also be passed through `via_tuple/1` to create an appropriate `{:via, _, _}` registry tuple to be passed as the `:name` option in `GenServer.start_link/3`.
      """
      def start_link(term) do
        case GenServer.start_link(__MODULE__, term, name: via_tuple(term)) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, _}} -> :ignore
          error -> error
        end
      end

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
      Returns the PID of a Horde Process by its unique process id. If no such process is registered, this will return `nil`.
      """
      @spec fetch(term()) :: pid() | nil
      def fetch(term) do
        case Horde.Registry.lookup(unquote(registry), process_id(term)) do
          [{pid, _}] -> pid
          [] -> nil
        end
      end

      @doc """
      Similar to `fetch/1` but will attempt to start a new Horde Process if one is not already running.

      The maximum amount of time this function can take to return is determined by the "Wait Options" when using `use Horde.Process`. If the process is not registered within that time, this will return `{:error, :not_found}`.
      """
      @spec get(term()) :: {:ok, pid()} | {:error, term()}
      def get(term) do
        case fetch(term) do
          nil ->
            case start(term) do
              :ignore ->
                wait_for_init(term, 1)

              res ->
                res
            end

          pid ->
            {:ok, pid}
        end
      end

      @doc """
      A simple recursive function that will attempt to fetch a PID for a given term, waiting a short period of time between attempts.

      It's possible to fail to fetch a PID as well as get an `:ignore` response if the child process has not completed `init/1`. To understand why, we need to look at how Horde starts and registers processes. Let's assume that we are using `get/1` to either fetch the PID of an existing process or start a new process.

      1. We use the `process_id/1` function to determine a unique identifier for the process.
      2. Horde attempts to look up that identifier in the registry. If not found, it moves on to the next step.
      3. Horde uses DeltaCRDT to determine which node that identifier is associated with.
      4. Horde sends a `start_link` RPC payload to the remote node to start a new process.
      5. Once the `init/1` function has completed, Horde receives the PID and adds it to the registry.

      The above assumes that the process on the remote node is not already being initialized. If `init/1` hasn't completed yet, or the registry hasn't completed replication to the local node, the RPC payload will return `:ignore`. This is because the remote node is aware that the unique process name is already in use even if the local node doesn't have access to that information yet. Therefore, you cannot fetch the PID because it's not in the registry yet, but you also can't start the process because it's already in the process of being started. This is why using `{:continue, term}` in the `init/1` function is crucial to ensure the process is started and registered as quickly as possible.

      However, even if `init/1` does practically no work, it's still possible for applications with a high degree of concurrency to try to access the same unique process at the same time. To work around this, `wait_for_init/2` will sleep for a short period of time between `fetch/1` calls to see if the remote process can finish its init phase and be added to the registry. If the process is not registered within the configured amount of attempts, `wait_for_init/2` will return `{:error, :not_found}`.
      """
      def wait_for_init(term, attempt) when attempt <= unquote(wait_max) do
        case fetch(term) do
          nil ->
            Process.sleep(unquote(wait_sleep))
            wait_for_init(term, attempt + 1)

          pid ->
            {:ok, pid}
        end
      end

      def wait_for_init(_, _), do: {:error, :not_found}

      @doc """
      Attempts to invoke `GenServer.call/2` on a Horde Process.

      If no registered process exists, this will return `{:error, :not_found}`. If the process does exist, `{:ok, response}` is returned, where `response` is the response from `GenServer.call/2`.

      *Note*: Because of how this function wraps the response of `GenServer.call/2`, it's possible to get a result tuple that looks like the following:

          {:ok, {:error, :invalid}}

      Because the first element of the tuple is `:ok`, that means a valid process was found and received the message. The second element, `{:error, :invalid}`, is the response from the process. If you match on the result of this function, be sure to match on both the first and second elements separately.
      """
      def call(term, message) do
        case fetch(term) do
          nil ->
            {:error, :not_found}

          pid ->
            {:ok, GenServer.call(pid, message)}
        end
      end

      @doc """
      Executes `GenServer.call/2` on a Horde Process, starting a new process if necessary.

      If the process is not running and cannot be started, this function will result in an exception.
      """
      def call!(term, message) do
        {:ok, pid} = get(term)
        GenServer.call(pid, message)
      end

      @doc """
      Attempts to invoke `GenServer.cast/2` on a Horde Process.

      If no registered process exists, this will return `nil`. Otherwise, it will return `:ok`.
      """
      def cast(term, message) do
        case fetch(term) do
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
      def cast!(term, message) do
        {:ok, pid} = get(term)
        GenServer.cast(pid, message)
      end
    end
  end
end
