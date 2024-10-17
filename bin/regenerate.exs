#!/usr/bin/env iex

require Logger

Mix.install(
  [
    {:csv, ">= 0.0.0"},
    {:ex_data_catalog, ">= 0.0.0"},
    {:geohash, "~> 1.0"},
    {:jason, ">= 0.0.0"},
    {:req, ">= 0.0.0"}
  ],
  consolidate_protocols: false
)

Application.ensure_started(:geohash)

defmodule LongestCommonPrefix do
  def find(text_1, text_2), do: head(text_1, text_2, [])

  defp head(<<ch_1, tail_1::binary>>, <<ch_2, tail_2::binary>>, prefix) when ch_1 == ch_2 do
    head(tail_1, tail_2, [ch_1 | prefix])
  end

  defp head(_string_1, _string_2, prefix) do
    prefix
    |> Enum.reverse()
    |> to_string()
  end
end

defmodule Fetcher do
  def fetch(url, path) do
    stream = File.stream!(path, [:delayed_write])
    Req.get!(url, into: stream)
  end
end

defmodule Downloads do
  def path(url) do
    Path.join("downloads", Path.basename(url))
  end

  def name(url) do
    url
    |> Path.basename()
    |> Path.rootname()
    |> String.replace(~r/^\d+\-/, "")
  end

  def extracted_path(url) do
    Path.join("downloads", name(url))
  end

  def stops_path(url) do
    url
    |> Downloads.extracted_path()
    |> Path.join("stops.txt")
  end
end

defmodule Zip do
  def extract(zip_path, extracted_path) do
    {:ok, _files} = :zip.unzip(to_charlist(zip_path), cwd: to_charlist(extracted_path))
  end
end

defmodule DMFR do
  @enforce_keys [:id, :urls]
  defstruct id: nil,
            urls: nil,
            license: %{
              use_without_attribution: "no",
              create_derived_product: "yes",
              attribution_text: "https://dati.toscana.it/"
            },
            tags: %{
              unstable_url: "true"
            },
            operators: nil,
            supersedes_ids: nil

  def name(%DMFR{} = dmfr) do
    dmfr.id
    |> String.split("-")
    |> Enum.at(-1)
  end

  defimpl Jason.Encoder do
    def encode(dmfr, opts) do
      json = %{
        id: dmfr.id,
        spec: "gtfs",
        urls: dmfr.urls,
        license: dmfr.license,
        tags: dmfr.tags
      }

      json =
        if dmfr.operators do
          Map.put(json, :operators, dmfr.operators)
        else
          json
        end

      json =
        if dmfr.supersedes_ids do
          Map.put(json, :supersedes_ids, dmfr.supersedes_ids)
        else
          json
        end

      json
      |> Jason.Encode.map(opts)
    end
  end
end

defmodule Feed do
  @feed_prefix "f"
  @default_geohash "s"

  def dmfr(url) do
    ensure_download(url)
    ensure_extracted(url)

    %DMFR{id: id(url), urls: %{static_current: url}}
  end

  defp ensure_download(url) do
    zip_path = Downloads.path(url)

    if !File.exists?(zip_path) do
      Logger.debug("Downloading: #{url}")
      Fetcher.fetch(url, zip_path)
    end
  end

  defp ensure_extracted(url) do
    extracted_path = Downloads.extracted_path(url)

    if !File.exists?(extracted_path) do
      zip_path = Downloads.path(url)
      Logger.debug("Extracting: #{zip_path}")
      Zip.extract(zip_path, extracted_path)
    end
  end

  defp id(url) do
    geohash = geohash(url)
    name = Downloads.name(url)
    "#{@feed_prefix}-#{geohash}-#{name}"
  end

  defp geohash(url) do
    {{min_lat, min_lon}, {max_lat, max_lon}} =
      bounds(url)

    bottom_left_geohash = Geohash.encode(min_lat, min_lon)
    top_right_geohash = Geohash.encode(max_lat, max_lon)

    case LongestCommonPrefix.find(bottom_left_geohash, top_right_geohash) do
      "" -> @default_geohash
      common_prefix -> common_prefix
    end
  end

  defp bounds(url) do
    url
    |> Downloads.stops_path()
    |> File.stream!()
    |> CSV.decode!(headers: true)
    |> Stream.map(&{&1["stop_lat"] |> String.to_float(), &1["stop_lon"] |> String.to_float()})
    |> Enum.to_list()
    |> Enum.reduce(
      {{Float.max_finite(), Float.max_finite()}, {Float.min_finite(), Float.min_finite()}},
      fn {lat, lon}, {{min_lat, min_lon}, {max_lat, max_lon}} ->
        {{min(min_lat, lat), min(min_lon, lon)}, {max(max_lat, lat), max(max_lon, lon)}}
      end
    )
  end
end

source_url = "https://dati.toscana.it/dataset/rt-oraritb.xml"
source_path = Downloads.path(source_url)

if !File.exists?(source_path) do
  Logger.debug("Downloading source data: #{source_url}")
  Fetcher.fetch(source_url, source_path)
end

catalog = ExDataCatalog.load(source_path)

feeds =
  Enum.map(catalog.distributions, fn distribution ->
    Feed.dmfr(distribution.access_url)
  end)

dmfr =
  %{
    "$schema": "https://dmfr.transit.land/json-schema/dmfr.schema-v0.5.0.json",
    feeds: feeds,
    license_spdx_identifier: "CDLA-Permissive-1.0"
  }
  |> Jason.encode!(pretty: true)

File.write!("dati.toscana.it.dmfr.json", dmfr)

Logger.flush()
System.halt()
