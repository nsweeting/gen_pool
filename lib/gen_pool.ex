defmodule GenPool do
  ################################
  # Public API
  ################################

  @spec start_link(module(), mfa(), keyword(), GenServer.options()) :: GenServer.on_start()
  def start_link(module, start_mfa, opts \\ [], server_opts \\ []) do
    GenPool.Server.start_link(module, start_mfa, opts, server_opts)
  end

  @spec checkout(GenServer.name(), boolean(), timeout()) :: {:ok, pid()} | :empty
  def checkout(pool, block \\ true, timeout \\ 5_000) do
    GenServer.call(pool, {:checkout, block}, timeout)
  end

  @spec checkin(GenServer.name(), pid()) :: :ok
  def checkin(pool, worker) do
    GenServer.cast(pool, {:checkin, worker})
  end

  @spec transaction(GenServer.name(), fun(), timeout()) :: any()
  def transaction(pool, fun, timeout \\ 5_000) do
    {:ok, worker} = checkout(pool, true, timeout)

    try do
      fun.(worker)
    after
      :ok = checkin(pool, worker)
    end
  end

  @spec state(GenServer.name(), timeout()) :: map()
  def state(pool, timeout \\ 5_000) do
    GenServer.call(pool, :state, timeout)
  end
end
