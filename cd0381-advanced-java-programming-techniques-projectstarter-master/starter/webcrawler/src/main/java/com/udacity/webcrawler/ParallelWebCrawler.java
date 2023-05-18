package com.udacity.webcrawler;
import com.udacity.webcrawler.json.CrawlResult;
import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ForkJoinPool;;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;
import java.util.concurrent.ConcurrentMap;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final PageParserFactory parserFactory;
  private final List<Pattern> ignoredUrls;
  @Inject
  ParallelWebCrawler(
          Clock clock,
          PageParserFactory parserFactory,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @IgnoredUrls List<Pattern> ignoredUrls,
          @MaxDepth int maxDepth
  ) {
    this.clock = clock;
    this.ignoredUrls = ignoredUrls;
    this.timeout = timeout;
    this.maxDepth = maxDepth;
    this.parserFactory = parserFactory;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
  }
  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    Map<String, Integer> counts = Collections.synchronizedMap(new HashMap<>());
    Set<String> visitedUrls = Collections.synchronizedSet(new HashSet<>());
    for (String url : startingUrls) {
      pool.invoke(new CrawParallel(url, deadline, counts, visitedUrls, maxDepth));
    }
    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }
    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }
  private class CrawParallel extends RecursiveTask {
    final String url;
    Instant deadline;
    int maxDepth;
    Map<String, Integer> counts;
    Set<String> visitedUrls;
    CrawParallel(String url, Instant deadline, Map<String, Integer> counts, Set<String> visitedUrls, int maxDepth){
      this.url = url;
      this.deadline = deadline;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
      this.maxDepth = maxDepth;
    }
    @Override
    protected Set<String> compute(){
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return new HashSet<String>();
      }
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return new HashSet<String>();
        }
      }
      if (visitedUrls.contains(url)) {
        return new HashSet<String>();
      }
      visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();
      for (ConcurrentMap.Entry<String, Integer> a : result.getWordCounts().entrySet()) {
        counts.compute(a.getKey(), (k, v) -> (v == null) ? a.getValue() : a.getValue() + v);
      }
      List<CrawParallel> tasks = new ArrayList<>();
      for (String links : result.getLinks()) {
        tasks.add(new CrawParallel(links, deadline, counts,visitedUrls,maxDepth - 1));
      }
      invokeAll(tasks);
      return visitedUrls;
    }

  }
  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}