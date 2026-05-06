#include <chrono>
#include <databento/dbn.hpp>
#include <databento/dbn_encoder.hpp>
#include <databento/dbn_store.hpp>
#include <databento/file_stream.hpp>
#include <databento/historical.hpp>
#include <databento/pretty.hpp>
#include <databento/publishers.hpp>
#include <databento/record.hpp>
#include <databento/symbol_map.hpp>
#include <iomanip>
#include <iostream>

namespace db = databento;

int main() {

  // Historical client instead of LiveBlocking
  auto client = db::Historical::Builder()
                    .SetKey("db-6sPEiVNfcPyJWP7RRQfckaFmYLFKr")
                    .Build();

  // Historical fetch replaces live Subscribe + Start
  auto historical_store = client.TimeseriesGetRange(
      "GLBX.MDP3",
      db::DateTimeRange<std::string>{
          "2024-01-15T10:00:20",
          "2024-01-15T10:00:30"
      },
      {"ES.c.0", "ES.c.1", "CL.c.0", "CL.c.1"},
      db::Schema::Ohlcv1S,
      db::SType::Continuous,
      db::SType::InstrumentId,
      0);

  // SAME PATTERN AS LIVE:
  // write records into DBN first
  {
    db::OutFileStream out_file{"example.dbn"};

    db::DbnEncoder encoder{
        historical_store.GetMetadata(),
        &out_file
    };

    while (auto* record = historical_store.NextRecord()) {
      encoder.EncodeRecord(*record);
    }
  }

  // SAME AS LIVE:
  // reopen DBN file and replay it
  db::DbnStore store{"example.dbn"};

  std::cout << std::left << std::setw(32) << "ts_event" << std::right
            << std::setw(16) << "instrument_id"
            << std::setw(12) << "open"
            << std::setw(12) << "high"
            << std::setw(12) << "low"
            << std::setw(12) << "close"
            << std::setw(10) << "volume"
            << "  symbol\n";

  // db::PitSymbolMap symbol_map;
  auto symbol_map = store.GetMetadata().CreateSymbolMapForDate(
    date::year_month_day{
        date::year{2024},
        date::month{1},
        date::day{15}
    });

  while (auto* record = store.NextRecord()) {

    symbol_map.OnRecord(*record);

    if (auto* ohlcv = record->GetIf<db::OhlcvMsg>()) {

      std::cout << std::left
                << std::setw(32)
                << db::pretty::Ts{ohlcv->hd.ts_event}

                << std::right
                << std::setw(16)
                << ohlcv->hd.instrument_id

                << std::setprecision(2)

                << std::setw(12)
                << db::pretty::Px{ohlcv->open}

                << std::setw(12)
                << db::pretty::Px{ohlcv->high}

                << std::setw(12)
                << db::pretty::Px{ohlcv->low}

                << std::setw(12)
                << db::pretty::Px{ohlcv->close}

                << std::setw(10)
                << ohlcv->volume

                << "  "
                << symbol_map.At(*ohlcv)
                << '\n';
    }
  }

  return 0;
}