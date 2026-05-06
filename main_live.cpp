#include <chrono>
#include <databento/dbn.hpp>
#include <databento/dbn_encoder.hpp>
#include <databento/dbn_store.hpp>
#include <databento/file_stream.hpp>  // OutFileStream
#include <databento/live.hpp>         // LiveBuilder
#include <databento/pretty.hpp>
#include <databento/publishers.hpp>  // Dataset
#include <databento/record.hpp>      // Record
#include <databento/symbol_map.hpp>  // PitSymbolMap
#include <iomanip>
#include <iostream>

namespace db = databento;

int main() {
  auto client = db::LiveBlocking::Builder()
                    .SetKey("YOUR_API_KEY")
                    .SetDataset(db::Dataset::GlbxMdp3)
                    .BuildBlocking();
  // First, we subscribe to the OHLCV-1s schema for a few symbols
  client.Subscribe({"ES.c.0", "ES.c.1", "CL.c.0", "CL.c.1"},
                   db::Schema::Ohlcv1S, db::SType::Continuous);
  // We start streaming
  db::Metadata metadata = client.Start();
  {
    // We open a file for writing and encode the metadata
    db::OutFileStream out_file{"example.dbn"};
    db::DbnEncoder encoder{metadata, &out_file};
    // Wait for at least one record and 5 seconds before closing the connection
    const std::chrono::seconds timeout{5};
    const auto start = std::chrono::steady_clock::now();
    while (auto* record = client.NextRecord(timeout)) {
      encoder.EncodeRecord(*record);
      if (std::chrono::steady_clock::now() - start >= timeout) {
        break;
      }
    }
  }
  // Finally, we open the DBN file
  db::DbnStore store{"example.dbn"};
  std::cout << std::left << std::setw(32) << "ts_event" << std::right
            << std::setw(16) << "instrument_id" << std::setw(12) << "open"
            << std::setw(12) << "high" << std::setw(12) << "low"
            << std::setw(12) << "close" << std::setw(10) << "volume" << "  "
            << "symbol\n";
  db::PitSymbolMap symbol_map;
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

