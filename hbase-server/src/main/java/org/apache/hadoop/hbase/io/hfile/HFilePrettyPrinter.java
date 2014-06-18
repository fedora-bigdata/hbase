
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Implements pretty-printing functionality for {@link HFile}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HFilePrettyPrinter {

  private static final Log LOG = LogFactory.getLog(HFilePrettyPrinter.class);

  private Options options = new Options();

  private boolean verbose;
  private boolean printValue;
  private boolean printKey;
  private boolean shouldPrintMeta;
  private boolean printBlocks;
  private boolean printStats;
  private boolean checkRow;
  private boolean checkFamily;
  private boolean isSeekToRow = false;

  /**
   * The row which the user wants to specify and print all the KeyValues for.
   */
  private byte[] row = null;
  private Configuration conf;

  private List<Path> files = new ArrayList<Path>();
  private int count;

  private static final String FOUR_SPACES = "    ";

  public HFilePrettyPrinter() {
    options.addOption("v", "verbose", false,
        "Verbose output; emits file and meta data delimiters");
    options.addOption("p", "printkv", false, "Print key/value pairs");
    options.addOption("e", "printkey", false, "Print keys");
    options.addOption("m", "printmeta", false, "Print meta data of file");
    options.addOption("b", "printblocks", false, "Print block index meta data");
    options.addOption("k", "checkrow", false,
        "Enable row order check; looks for out-of-order keys");
    options.addOption("a", "checkfamily", false, "Enable family check");
    options.addOption("f", "file", true,
        "File to scan. Pass full-path; e.g. hdfs://a:9000/hbase/hbase:meta/12/34");
    options.addOption("w", "seekToRow", true,
      "Seek to this row and print all the kvs for this row only");
    options.addOption("r", "region", true,
        "Region to scan. Pass region name; e.g. 'hbase:meta,,1'");
    options.addOption("s", "stats", false, "Print statistics");
  }

  public boolean parseOptions(String args[]) throws ParseException,
      IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HFile", options, true);
      return false;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    verbose = cmd.hasOption("v");
    printValue = cmd.hasOption("p");
    printKey = cmd.hasOption("e") || printValue;
    shouldPrintMeta = cmd.hasOption("m");
    printBlocks = cmd.hasOption("b");
    printStats = cmd.hasOption("s");
    checkRow = cmd.hasOption("k");
    checkFamily = cmd.hasOption("a");

    if (cmd.hasOption("f")) {
      files.add(new Path(cmd.getOptionValue("f")));
    }

    if (cmd.hasOption("w")) {
      String key = cmd.getOptionValue("w");
      if (key != null && key.length() != 0) {
        row = key.getBytes();
        isSeekToRow = true;
      } else {
        System.err.println("Invalid row is specified.");
        System.exit(-1);
      }
    }

    if (cmd.hasOption("r")) {
      String regionName = cmd.getOptionValue("r");
      byte[] rn = Bytes.toBytes(regionName);
      byte[][] hri = HRegionInfo.parseRegionName(rn);
      Path rootDir = FSUtils.getRootDir(conf);
      Path tableDir = FSUtils.getTableDir(rootDir, TableName.valueOf(hri[0]));
      String enc = HRegionInfo.encodeRegionName(rn);
      Path regionDir = new Path(tableDir, enc);
      if (verbose)
        System.out.println("region dir -> " + regionDir);
      List<Path> regionFiles = HFile.getStoreFiles(FileSystem.get(conf),
          regionDir);
      if (verbose)
        System.out.println("Number of region files found -> "
            + regionFiles.size());
      if (verbose) {
        int i = 1;
        for (Path p : regionFiles) {
          if (verbose)
            System.out.println("Found file[" + i++ + "] -> " + p);
        }
      }
      files.addAll(regionFiles);
    }

    return true;
  }

  /**
   * Runs the command-line pretty-printer, and returns the desired command
   * exit code (zero for success, non-zero for failure).
   */
  public int run(String[] args) {
    conf = HBaseConfiguration.create();
    try {
      FSUtils.setFsDefault(conf, FSUtils.getRootDir(conf));
      if (!parseOptions(args))
        return 1;
    } catch (IOException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    } catch (ParseException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    }

    // iterate over all files found
    for (Path fileName : files) {
      try {
        processFile(fileName);
      } catch (IOException ex) {
        LOG.error("Error reading " + fileName, ex);
      }
    }

    if (verbose || printKey) {
      System.out.println("Scanned kv count -> " + count);
    }

    return 0;
  }

  private void processFile(Path file) throws IOException {
    if (verbose)
      System.out.println("Scanning -> " + file);
    FileSystem fs = file.getFileSystem(conf);
    if (!fs.exists(file)) {
      System.err.println("ERROR, file doesnt exist: " + file);
    }

    HFile.Reader reader = HFile.createReader(fs, file, new CacheConfig(conf), conf);

    Map<byte[], byte[]> fileInfo = reader.loadFileInfo();

    KeyValueStatsCollector fileStats = null;

    if (verbose || printKey || checkRow || checkFamily || printStats) {
      // scan over file and read key/value's and check if requested
      HFileScanner scanner = reader.getScanner(false, false, false);
      fileStats = new KeyValueStatsCollector();
      boolean shouldScanKeysValues = false;
      if (this.isSeekToRow) {
        // seek to the first kv on this row
        shouldScanKeysValues = 
          (scanner.seekTo(KeyValue.createFirstOnRow(this.row).getKey()) != -1);
      } else {
        shouldScanKeysValues = scanner.seekTo();
      }
      if (shouldScanKeysValues)
        scanKeysValues(file, fileStats, scanner, row);
    }

    // print meta data
    if (shouldPrintMeta) {
      printMeta(reader, fileInfo);
    }

    if (printBlocks) {
      System.out.println("Block Index:");
      System.out.println(reader.getDataBlockIndexReader());
    }

    if (printStats) {
      fileStats.finish();
      System.out.println("Stats:\n" + fileStats);
    }

    reader.close();
  }

  private void scanKeysValues(Path file, KeyValueStatsCollector fileStats,
      HFileScanner scanner,  byte[] row) throws IOException {
    KeyValue pkv = null;
    do {
      KeyValue kv = scanner.getKeyValue();
      if (row != null && row.length != 0) {
        int result = Bytes.compareTo(kv.getRow(), row);
        if (result > 0) {
          break;
        } else if (result < 0) {
          continue;
        }
      }
      // collect stats
      if (printStats) {
        fileStats.collect(kv);
      }
      // dump key value
      if (printKey) {
        System.out.print("K: " + kv);
        if (printValue) {
          System.out.print(" V: " + Bytes.toStringBinary(kv.getValue()));
          int i = 0;
          List<Tag> tags = kv.getTags();
          for (Tag tag : tags) {
            System.out
                .print(String.format(" T[%d]: %s", i++, Bytes.toStringBinary(tag.getValue())));
          }
        }
        System.out.println();
      }
      // check if rows are in order
      if (checkRow && pkv != null) {
        if (Bytes.compareTo(pkv.getRow(), kv.getRow()) > 0) {
          System.err.println("WARNING, previous row is greater then"
              + " current row\n\tfilename -> " + file + "\n\tprevious -> "
              + Bytes.toStringBinary(pkv.getKey()) + "\n\tcurrent  -> "
              + Bytes.toStringBinary(kv.getKey()));
        }
      }
      // check if families are consistent
      if (checkFamily) {
        String fam = Bytes.toString(kv.getFamily());
        if (!file.toString().contains(fam)) {
          System.err.println("WARNING, filename does not match kv family,"
              + "\n\tfilename -> " + file + "\n\tkeyvalue -> "
              + Bytes.toStringBinary(kv.getKey()));
        }
        if (pkv != null
            && !Bytes.equals(pkv.getFamily(), kv.getFamily())) {
          System.err.println("WARNING, previous kv has different family"
              + " compared to current key\n\tfilename -> " + file
              + "\n\tprevious -> " + Bytes.toStringBinary(pkv.getKey())
              + "\n\tcurrent  -> " + Bytes.toStringBinary(kv.getKey()));
        }
      }
      pkv = kv;
      ++count;
    } while (scanner.next());
  }

  /**
   * Format a string of the form "k1=v1, k2=v2, ..." into separate lines
   * with a four-space indentation.
   */
  private static String asSeparateLines(String keyValueStr) {
    return keyValueStr.replaceAll(", ([a-zA-Z]+=)",
                                  ",\n" + FOUR_SPACES + "$1");
  }

  private void printMeta(HFile.Reader reader, Map<byte[], byte[]> fileInfo)
      throws IOException {
    System.out.println("Block index size as per heapsize: "
        + reader.indexSize());
    System.out.println(asSeparateLines(reader.toString()));
    System.out.println("Trailer:\n    "
        + asSeparateLines(reader.getTrailer().toString()));
    System.out.println("Fileinfo:");
    for (Map.Entry<byte[], byte[]> e : fileInfo.entrySet()) {
      System.out.print(FOUR_SPACES + Bytes.toString(e.getKey()) + " = ");
      if (Bytes.compareTo(e.getKey(), Bytes.toBytes("MAX_SEQ_ID_KEY")) == 0) {
        long seqid = Bytes.toLong(e.getValue());
        System.out.println(seqid);
      } else if (Bytes.compareTo(e.getKey(), Bytes.toBytes("TIMERANGE")) == 0) {
        TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
        Writables.copyWritable(e.getValue(), timeRangeTracker);
        System.out.println(timeRangeTracker.getMinimumTimestamp() + "...."
            + timeRangeTracker.getMaximumTimestamp());
      } else if (Bytes.compareTo(e.getKey(), FileInfo.AVG_KEY_LEN) == 0
          || Bytes.compareTo(e.getKey(), FileInfo.AVG_VALUE_LEN) == 0) {
        System.out.println(Bytes.toInt(e.getValue()));
      } else {
        System.out.println(Bytes.toStringBinary(e.getValue()));
      }
    }

    try {
      System.out.println("Mid-key: " + Bytes.toStringBinary(reader.midkey()));
    } catch (Exception e) {
      System.out.println ("Unable to retrieve the midkey");
    }

    // Printing general bloom information
    DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();
    BloomFilter bloomFilter = null;
    if (bloomMeta != null)
      bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);

    System.out.println("Bloom filter:");
    if (bloomFilter != null) {
      System.out.println(FOUR_SPACES + bloomFilter.toString().replaceAll(
          ByteBloomFilter.STATS_RECORD_SEP, "\n" + FOUR_SPACES));
    } else {
      System.out.println(FOUR_SPACES + "Not present");
    }

    // Printing delete bloom information
    bloomMeta = reader.getDeleteBloomFilterMetadata();
    bloomFilter = null;
    if (bloomMeta != null)
      bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);

    System.out.println("Delete Family Bloom filter:");
    if (bloomFilter != null) {
      System.out.println(FOUR_SPACES
          + bloomFilter.toString().replaceAll(ByteBloomFilter.STATS_RECORD_SEP,
              "\n" + FOUR_SPACES));
    } else {
      System.out.println(FOUR_SPACES + "Not present");
    }
  }

  private static class KeyValueStatsCollector {
    private final MetricRegistry metricsRegistry = new MetricRegistry();
    private final ByteArrayOutputStream metricsOutput = new ByteArrayOutputStream();
    private final SimpleReporter simpleReporter = new SimpleReporter(metricsRegistry, new PrintStream(metricsOutput));
    Histogram keyLen = metricsRegistry.histogram("Key length");
    Histogram valLen = metricsRegistry.histogram("Val length");
    Histogram rowSizeBytes = metricsRegistry.histogram("Row size (bytes)");
    Histogram rowSizeCols = metricsRegistry.histogram("Row size (columns)");

    long curRowBytes = 0;
    long curRowCols = 0;

    byte[] biggestRow = null;

    private KeyValue prevKV = null;
    private long maxRowBytes = 0;
    private long curRowKeyLength;

    public void collect(KeyValue kv) {
      valLen.update(kv.getValueLength());
      if (prevKV != null &&
          KeyValue.COMPARATOR.compareRows(prevKV, kv) != 0) {
        // new row
        collectRow();
      }
      curRowBytes += kv.getLength();
      curRowKeyLength = kv.getKeyLength();
      curRowCols++;
      prevKV = kv;
    }

    private void collectRow() {
      rowSizeBytes.update(curRowBytes);
      rowSizeCols.update(curRowCols);
      keyLen.update(curRowKeyLength);

      if (curRowBytes > maxRowBytes && prevKV != null) {
        biggestRow = prevKV.getRow();
        maxRowBytes = curRowBytes;
      }

      curRowBytes = 0;
      curRowCols = 0;
    }

    public void finish() {
      if (curRowCols > 0) {
        collectRow();
      }
    }

    @Override
    public String toString() {
      if (prevKV == null)
        return "no data available for statistics";

      // Dump the metrics to the output stream
      simpleReporter.stop();
      simpleReporter.report();

      return
              metricsOutput.toString() +
                      "Key of biggest row: " + Bytes.toStringBinary(biggestRow);
    }
  }

  private static class SimpleReporter extends ScheduledReporter {
    private final PrintStream out;
    private final Locale locale = Locale.getDefault();

    public SimpleReporter(MetricRegistry metricsRegistry, PrintStream out) {
      super(metricsRegistry, "simple-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
      this.out = out;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
      if (!gauges.isEmpty()) {
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
          out.print("   " + entry.getKey());
          out.println(':');
          processGauge(entry);
        }
      }

      if (!counters.isEmpty()) {
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
          out.print("   " + entry.getKey());
          out.println(':');
          processCounter(entry);
        }
      }

      if (!histograms.isEmpty()) {
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
          out.print("   " + entry.getKey());
          out.println(':');
          processHistogram(entry.getValue());
        }
      }

      if (!meters.isEmpty()) {
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
          out.print("   " + entry.getKey());
          out.println(':');
          processMeter(entry.getValue());
        }
      }

      if (!timers.isEmpty()) {
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
          out.print("   " + entry.getKey());
          out.println(':');
          processTimer(entry.getValue());
        }
      }
    }

    private void processGauge(Map.Entry<String, Gauge> entry) {
        out.printf(locale, "    value = %s\n", entry.getValue().getValue());
    }

    private void processCounter(Map.Entry<String, Counter> entry) {
        out.printf(locale, "    count = %d\n", entry.getValue().getCount());
    }

    private void processMeter(Meter meter) {
        final String rateUnit = getRateUnit();
        out.printf(locale, "             count = %d\n", meter.getCount());
        out.printf(locale, "         mean rate = %2.2f events/%s\n",
                      convertRate(meter.getMeanRate()), rateUnit);
        out.printf(locale, "     1-minute rate = %2.2f events/%s\n",
                      convertRate(meter.getOneMinuteRate()), rateUnit);
        out.printf(locale, "     5-minute rate = %2.2f events/%s\n",
                      convertRate(meter.getFiveMinuteRate()), rateUnit);
        out.printf(locale, "    15-minute rate = %2.2f events/%s\n",
                      convertRate(meter.getFifteenMinuteRate()), rateUnit);
    }

    private void processHistogram(Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();
        out.printf(locale, "               min = %2.2f\n", snapshot.getMin());
        out.printf(locale, "               max = %2.2f\n", snapshot.getMax());
        out.printf(locale, "              mean = %2.2f\n", snapshot.getMean());
        out.printf(locale, "            stddev = %2.2f\n", snapshot.getStdDev());
        out.printf(locale, "            median = %2.2f\n", snapshot.getMedian());
        out.printf(locale, "              75%% <= %2.2f\n", snapshot.get75thPercentile());
        out.printf(locale, "              95%% <= %2.2f\n", snapshot.get95thPercentile());
        out.printf(locale, "              98%% <= %2.2f\n", snapshot.get98thPercentile());
        out.printf(locale, "              99%% <= %2.2f\n", snapshot.get99thPercentile());
        out.printf(locale, "            99.9%% <= %2.2f\n", snapshot.get999thPercentile());
        out.printf(locale, "             count = %d\n", histogram.getCount());
    }

    private void processTimer(Timer timer) {
        final String durationUnit = getDurationUnit();
        final String rateUnit = getRateUnit();
        final Snapshot snapshot = timer.getSnapshot();
        out.printf(locale, "             count = %d\n", timer.getCount());
        out.printf(locale, "         mean rate = %2.2f events/%s\n",
                      convertRate(timer.getMeanRate()), rateUnit);
        out.printf(locale, "     1-minute rate = %2.2f events/%s\n",
                      convertRate(timer.getOneMinuteRate()), rateUnit);
        out.printf(locale, "     5-minute rate = %2.2f events/%s\n",
                      convertRate(timer.getFiveMinuteRate()), rateUnit);
        out.printf(locale, "    15-minute rate = %2.2f events/%s\n",
                      convertRate(timer.getFifteenMinuteRate()), rateUnit);

        out.printf(locale, "               min = %2.2f%s\n", convertDuration(snapshot.getMin()), durationUnit);
        out.printf(locale, "               max = %2.2f%s\n", convertDuration(snapshot.getMax()), durationUnit);
        out.printf(locale, "              mean = %2.2f%s\n", convertDuration(snapshot.getMean()), durationUnit);
        out.printf(locale, "            stddev = %2.2f%s\n", convertDuration(snapshot.getStdDev()), durationUnit);
        out.printf(locale, "            median = %2.2f%s\n", convertDuration(snapshot.getMedian()), durationUnit);
        out.printf(locale, "              75%% <= %2.2f%s\n", convertDuration(snapshot.get75thPercentile()), durationUnit);
        out.printf(locale, "              95%% <= %2.2f%s\n", convertDuration(snapshot.get95thPercentile()), durationUnit);
        out.printf(locale, "              98%% <= %2.2f%s\n", convertDuration(snapshot.get98thPercentile()), durationUnit);
        out.printf(locale, "              99%% <= %2.2f%s\n", convertDuration(snapshot.get99thPercentile()), durationUnit);
        out.printf(locale, "            99.9%% <= %2.2f%s\n", convertDuration(snapshot.get999thPercentile()), durationUnit);
    }



/*
    public void run() {
      for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(
              MetricFilter.ALL).entrySet()) {
        try {
          for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
            out.print("   " + subEntry.getKey().getName());
            out.println(':');

            subEntry.getValue().processWith(this, subEntry.getKey(), out);
          }
        } catch (Exception e) {
          e.printStackTrace(out);
        }
      }
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, PrintStream stream) {
      super.processHistogram(name, histogram, stream);
      stream.printf(Locale.getDefault(), "             count = %d\n", histogram.getCount());
    }
*/
  }
}
