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

supersedes_ids = %{
  "trenitaliaspa" => ["f-sp-trenitaliaspa"],
  "urbanopiombino" => ["f-spx-tiemmespa"],
  "extraurbanoprato" => ["f-spz-capconsorzioautolineepratesi"],
  "extraurbanopisa" => ["f-spz-consorziopisanotrasporti"],
  "extraurbanolivorno" => ["f-spz-cttnord"],
  "extraurbanolucca" => ["f-spz-vaibus"],
  "extraurbanofirenze" => [
    "f-spzb-piùbus",
    "f-srb-autolineemugellovaldisieve",
    "f-srb0-autolineechiantivaldarno"
  ],
  "extraurbanoarezzo" => ["f-sr8-etruriamobilità"],
  "extraurbanosiena" => ["f-sr8-sienamobilità"]
}

feeds =
  Enum.map(catalog.distributions, fn distribution ->
    Feed.dmfr(distribution.access_url)
  end)
  |> Enum.map(fn dmfr ->
    name = DMFR.name(dmfr)
    supersedes = supersedes_ids[name]

    if supersedes do
      struct!(dmfr, supersedes_ids: supersedes)
    else
      dmfr
    end
  end)

defmodule Historical do
  def feeds do
    [
      %DMFR{
        id: "f-spx-toremartoscanaregionalemarittimaspa",
        urls: %{
          static_current:
            "https://dati.toscana.it/dataset/8bb8f8fe-fe7d-41d0-90dc-49f2456180d1/resource/56539a5a-e0be-49eb-b3ac-052a42ad0de0/download/toremar.gtfs.zip",
          static_historic: [
            "http://dati.toscana.it/dataset/8bb8f8fe-fe7d-41d0-90dc-49f2456180d1/resource/ad82f56d-7bd9-4695-bc56-4a0e134e09cf/download/toremar.gtfs"
          ]
        },
        operators: [
          %{
            onestop_id: "o-spx-toremartoscanaregionalemarittimaspa",
            name: "Toremar Toscana Regionale Marittima",
            website: "http://www.toremar.it",
            associated_feeds: [
              %{
                gtfs_agency_id: "205"
              }
            ]
          }
        ]
      },
      %DMFR{
        id: "f-spzbz-gestspa",
        urls: %{
          static_current:
            "http://dati.toscana.it/dataset/8bb8f8fe-fe7d-41d0-90dc-49f2456180d1/resource/aab11416-324e-4199-9ffc-857cc0599a2a/download/gest.gtfs"
        },
        operators: [
          %{
            onestop_id: "o-spzbz-gestspa",
            name: "GEST S.p.A.",
            short_name: "GEST",
            website: "http://www.gestramvia.it",
            associated_feeds: [
              %{
                gtfs_agency_id: "303"
              }
            ]
          }
        ]
      },
      %DMFR{
        id: "f-spzc-ataf~linea",
        urls: %{
          static_current:
            "http://dati.toscana.it/dataset/8bb8f8fe-fe7d-41d0-90dc-49f2456180d1/resource/ee55333c-fe53-4599-981d-389b13f28bb1/download/ataflinea.gtfs"
        },
        operators: [
          %{
            onestop_id: "o-spzc-ataf~linea",
            name: "Azienda Trasporti Area Fiorentina",
            short_name: "ATAF",
            website: "http://www.ataf.net",
            associated_feeds: [
              %{
                gtfs_agency_id: "172"
              }
            ]
          }
        ]
      },
      %DMFR{
        id: "f-sr8-tftspa",
        urls: %{
          static_current:
            "http://dati.toscana.it/dataset/8bb8f8fe-fe7d-41d0-90dc-49f2456180d1/resource/0a8e7c64-2314-4732-849a-67746b8a0eba/download/tft.gtfs"
        },
        operators: [
          %{
            onestop_id: "o-sr8-tftspa",
            name: "Trasporto Ferroviario Toscano",
            short_name: "TFT",
            website: "http://www.trasportoferroviariotoscano.it",
            associated_feeds: [
              %{
                gtfs_agency_id: "196"
              }
            ]
          }
        ]
      }
    ]
  end
end

dmfr =
  %{
    "$schema": "https://dmfr.transit.land/json-schema/dmfr.schema-v0.5.0.json",
    feeds: feeds ++ Historical.feeds(),
    license_spdx_identifier: "CDLA-Permissive-1.0"
  }
  |> Jason.encode!(pretty: true)

File.write!("dati.toscana.it.dmfr.json", dmfr)

Logger.flush()
System.halt()
