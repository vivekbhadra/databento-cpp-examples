#include <chrono>
#include <databento/dbn.hpp>
#include <databento/dbn_encoder.hpp>
#include <databento/dbn_store.hpp>
#include <databento/file_stream.hpp>
#include <databento/historical.hpp>   // was live.hpp
#include <databento/pretty.hpp>
#include <databento/publishers.hpp>
#include <databento/record.hpp>
#include <databento/symbol_map.hpp>
#include <iomanip>
#include <iostream>

namespace db = databento;

int main() {
  auto client = db::Historical::Builder()   // was LiveBlocking::Builder
                    .SetKey("db-BE4LqkdwnG5mWuBcpxHqgaL7cBPey")
                    .Build();               // was BuildBlocking

  auto store = client.TimeseriesGetRange(   // replaces Subscribe+Start+loop+file
      "GLBX.MDP3",
      db::DateTimeRange<std::string>{"2024-01-15T14:30:00", "2024-01-15T14:32:00"},
      {"ES.c.0", "ES.c.1"},
      db::Schema::Ohlcv1S,
      db::SType::Continuous,
      db::SType::InstrumentId,
      0);

  // Everything below is 100% unchanged
  std::cout << std::left << std::setw(32) << "ts_event" << std::right
            << std::setw(16) << "instrument_id" << std::setw(12) << "open"
            << std::setw(12) << "high" << std::setw(12) << "low"
            << std::setw(12) << "close" << std::setw(10) << "volume" << "  "
            << "symbol\n";

  auto symbol_map = store.GetMetadata().CreateSymbolMapForDate(
    date::year_month_day{date::year{2024}, date::month{1}, date::day{15}});
while (auto* record = store.NextRecord()) {
    symbol_map.OnRecord(*record);
    if (auto* ohlcv = record->GetIf<db::OhlcvMsg>()) {
      std::cout << std::left << std::setw(32)
                << db::pretty::Ts{ohlcv->hd.ts_event} << std::right
                << std::setw(16) << ohlcv->hd.instrument_id
                << std::setprecision(2) << std::setw(12)
                << db::pretty::Px{ohlcv->open} << std::setw(12)
                << db::pretty::Px{ohlcv->high} << std::setw(12)
                << db::pretty::Px{ohlcv->low} << std::setw(12)
                << db::pretty::Px{ohlcv->close} << std::setw(10)
                << ohlcv->volume << "  " << symbol_map.At(*ohlcv) << '\n';
    }
  }
  return 0;
}