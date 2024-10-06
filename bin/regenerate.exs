#!/usr/bin/env iex

require Logger

Mix.install([
  {:csv, ">= 0.0.0"},
  {:geohash, "~> 1.0"},
  {:jason, ">= 0.0.0"},
  {:req, ">= 0.0.0"},
  {:sweet_xml, ">= 0.0.0"},
])

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

defmodule Feed do
  @enforce_keys [:url]
  defstruct [:url]

  @feed_prefix "f"
  @default_geohash "s"

  def dmfr(%__MODULE__{} = feed) do
    ensure_download(feed)
    ensure_extracted(feed)

    %{
      id: id(feed),
      spec: "gtfs",
      urls: %{
        static_current: feed.url
      },
      license: %{
        use_without_attribution: "no",
        create_derived_product: "yes",
        attribution_text: "https://dati.toscana.it/"
      },
      tags: %{
        unstable_url: "true"
      }
    }
  end

  defp ensure_download(%__MODULE__{} = feed) do
    zip_path = Downloads.path(feed.url)
    if !File.exists?(zip_path) do
      Logger.debug("Downloading: #{feed.url}")
      Fetcher.fetch(feed.url, zip_path)
    end
  end

  defp ensure_extracted(%__MODULE__{} = feed) do
    extracted_path = Downloads.extracted_path(feed.url)
    if !File.exists?(extracted_path) do
      zip_path = Downloads.path(feed.url)
      Logger.debug("Extracting: #{zip_path}")
      Zip.extract(zip_path, extracted_path)
    end
  end

  defp id(%__MODULE__{} = feed) do
    geohash = geohash(feed)
    name = Downloads.name(feed.url)
    "#{@feed_prefix}-#{geohash}-#{name}"
  end

  defp geohash(%__MODULE__{} = feed) do
    {{min_lat, min_lon}, {max_lat, max_lon}} =
      bounds(feed)
    bottom_left_geohash = Geohash.encode(min_lat, min_lon)
    top_right_geohash = Geohash.encode(max_lat, max_lon)
    case LongestCommonPrefix.find(bottom_left_geohash, top_right_geohash) do
      "" -> @default_geohash
      common_prefix -> common_prefix
    end
  end

  defp bounds(%__MODULE__{} = feed) do
    feed.url
    |> Downloads.stops_path()
    |> File.stream!()
    |> CSV.decode!(headers: true)
    |> Stream.map(& {&1["stop_lat"] |> String.to_float(), &1["stop_lon"] |> String.to_float()})
    |> Enum.to_list()
    |> Enum.reduce(
      {{Float.max_finite, Float.max_finite}, {Float.min_finite, Float.min_finite}},
      fn {lat, lon}, {{min_lat, min_lon}, {max_lat, max_lon}} ->
        {{min(min_lat, lat), min(min_lon, lon)}, {max(max_lat, lat), max(max_lon, lon)}}
      end
    )
  end
end

defmodule Feeds do
  import SweetXml, only: [sigil_x: 2]

  def parse_dataset(xml) do
    xml
    |> SweetXml.xpath(
      ~x"//rdf:RDF/dcat:Dataset/dcat:distribution/dcat:Distribution/dcat:accessURL/@rdf:resource"l
    )
    |> Enum.map(&to_string/1)
    |> Enum.map(& %Feed{url: &1})
  end
end

source_url = "https://dati.toscana.it/dataset/rt-oraritb.xml"
source_path = Downloads.path(source_url)
if !File.exists?(source_path) do
  Logger.debug("Downloading source data: #{source_url}")
  Fetcher.fetch(source_url, source_path)
end

available =
  source_path
  |> File.read!()
  |> Feeds.parse_dataset()

feeds =
  Enum.map(available, fn feed ->
    Feed.dmfr(feed)
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
