defmodule GenPool.Server do
  use GenServer

  @opts_schema %{
    max_size: [type: :integer, default: 1, required: true],
    strategy: [type: :atom, default: :fifo, inclusion: [:lifo, :fifo]]
  }

  ################################
  # Public API
  ################################

  @spec start_link(module(), mfa(), keyword(), GenServer.options()) :: GenServer.on_start()
  def start_link(module, start_mfa, opts \\ [], server_opts \\ []) do
    GenServer.start_link(__MODULE__, {module, start_mfa, opts}, server_opts)
  end

  ################################
  # GenServer Callbacks
  ################################

  @impl GenServer
  def init({module, start_mfa, opts}) do
    Process.flag(:trap_exit, true)

    with {:ok, opts} <- module_init(module, opts),
         {:ok, opts} <- validate_opts(opts) do
      state = module |> init_state(start_mfa, opts) |> start_workers()
      {:ok, state}
    end
  end

  @impl GenServer
  def handle_call({:checkout, block}, {pid, _}, state) do
    case checkout_worker(state, pid, block) do
      {:empty, state} when block == false ->
        {:reply, :empty, state}

      {:empty, state} ->
        {:noreply, state}

      {:ok, worker, state} ->
        {:reply, {:ok, worker}, state}
    end
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  @impl GenServer
  def handle_cast({:checkin, worker}, state) do
    state = checkin_worker(state, worker)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, _, _, _}, state) do
    state = monitor_down(state, ref)
    {:noreply, state}
  end

  def handle_info({:EXIT, worker, _}, state) do
    state = remove_worker(state, worker)
    {:noreply, state}
  end

  ################################
  # Private API
  ################################

  defp module_init(module, opts) do
    if Code.ensure_loaded?(module) and function_exported?(module, :init, 1) do
      module.init(opts)
    else
      {:ok, opts}
    end
  end

  defp validate_opts(opts) do
    KeywordValidator.validate(opts, @opts_schema)
  end

  defp init_state(module, start_mfa, opts) do
    %{
      module: module,
      max_size: Keyword.get(opts, :max_size),
      strategy: Keyword.get(opts, :strategy),
      start_mfa: start_mfa,
      supervisor: start_supervisor(),
      monitors: :ets.new(:monitors, [:private]),
      workers: :queue.new(),
      workers_size: 0,
      blockers: :queue.new(),
      blockers_size: 0
    }
  end

  defp start_supervisor do
    {:ok, pid} = DynamicSupervisor.start_link(strategy: :one_for_one)
    pid
  end

  defp start_workers(state), do: start_workers(state, state.max_size)
  defp start_workers(state, 0), do: state

  defp start_workers(state, max_size) do
    state = start_worker(state)
    start_workers(state, max_size - 1)
  end

  defp start_worker(state) do
    child_spec = worker_spec(state)
    {:ok, worker} = DynamicSupervisor.start_child(state.supervisor, child_spec)
    Process.link(worker)
    put_worker(state, worker)
  end

  defp put_worker(state, worker) do
    workers = :queue.in(worker, state.workers)
    %{state | workers: workers, workers_size: state.workers_size + 1}
  end

  defp checkout_worker(%{workers_size: 0} = state, _pid, false) do
    {:empty, state}
  end

  defp checkout_worker(%{workers_size: 0} = state, blocker, true) do
    state = put_blocker(state, blocker)
    {:empty, state}
  end

  defp checkout_worker(state, runner, _block) do
    {{_, worker}, workers} = get_from_queue(state.strategy, state.workers)
    state = %{state | workers: workers, workers_size: state.workers_size - 1}
    put_runner(state, runner, worker)
    {:ok, worker, state}
  end

  defp checkin_worker(state, worker) do
    case :ets.lookup(state.monitors, worker) do
      [{pid, ref}] ->
        true = Process.demonitor(ref)
        true = :ets.delete(state.monitors, pid)
        state |> put_worker(worker) |> reply_blocker()

      [] ->
        state
    end
  end

  defp remove_worker(state, worker) do
    with [{_worker, ref}] <- :ets.take(state.monitors, worker) do
      true = Process.demonitor(ref)
    end

    workers = :queue.filter(fn wpid -> wpid != worker end, state.workers)
    state = %{state | workers: workers, workers_size: state.workers_size - 1}
    start_worker(state)
  end

  defp put_blocker(state, blocker) do
    ref = Process.monitor(blocker)
    blockers = :queue.in({blocker, ref}, state.blockers)
    %{state | blockers: blockers, blockers_size: state.blockers_size + 1}
  end

  defp reply_blocker(%{blockers_size: 0} = state), do: state

  defp reply_blocker(state) do
    {{_, blocker}, blockers} = get_from_queue(:fifo, state.blockers)
    state = %{state | blockers: blockers, blockers_size: state.blockers_size - 1}

    case checkout_worker(state, blocker, true) do
      {:ok, worker, state} ->
        GenServer.reply(blocker, worker)
        state

      {:empty, state} ->
        state
    end
  end

  defp put_runner(state, runner, worker) do
    ref = Process.monitor(runner)
    :ets.insert(state.monitors, {worker, ref})
  end

  defp monitor_down(state, ref) do
    case :ets.match(state.monitors, {:"$1", ref}) do
      [[worker]] ->
        true = Process.demonitor(ref)
        true = :ets.delete(state.monitors, worker)
        put_worker(state, worker)

      [[]] ->
        state
    end
  end

  defp get_from_queue(:fifo, queue), do: :queue.out(queue)
  defp get_from_queue(:lifo, queue), do: :queue.out_r(queue)

  defp worker_spec(state) do
    %{
      id: make_ref(),
      start: state.start_mfa,
      restart: :temporary,
      type: :worker
    }
  end
end
