defmodule Horde.Process.MixProject do
  use Mix.Project

  def project do
    [
      app: :horde_process,
      description: """
      A helper module for constructing and managing processes using the Horde distributed process library.
      """,
      package: package(),
      source_url: "https://github.com/tyler-eon/horde-process",
      homepage_url: "https://github.com/tyler-eon/horde-process",
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
    ]
  end

  # For Hex packaging.
  defp package do
    [
      links: %{
        "GitHub" => "https://github.com/tyler-eon/horde-process",
        "HexDocs" => "https://hexdocs.pm/horde_process"
      },
      licenses: ["MIT"],
    ]
  end
end
