package uz.boqiyqahramonlar.hadoop_back_end.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uz.boqiyqahramonlar.hadoop_back_end.dto.LogEvent;
import uz.boqiyqahramonlar.hadoop_back_end.model.JobEntity;
import uz.boqiyqahramonlar.hadoop_back_end.model.JobStatus;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class TextAnalyzerService {

    @Value("${app.top-n:20}")
    private int topN;

    @Value("${app.min-token-length:2}")
    private int minTokenLength;

    @Value("${app.enable-stopwords:true}")
    private boolean enableStopwords;

    private final LogStreamService logStreamService;

    private static final Pattern SPLIT = Pattern.compile("[^\\p{L}\\p{Nd}]+");
    private static final Pattern DIACRITICS = Pattern.compile("\\p{M}+");

    // Minimal multi-language stopwords (xohlasangiz file'dan o'qiymiz keyin)
    private static final Set<String> STOPWORDS = Set.of(
            "the","a","an","and","or","to","of","in","on","for","is","are","was","were",
            "va","ham","bu","shu","bilan","uchun","lekin","emas","bor","yoq"
    );

    public TextAnalyzerService(LogStreamService logStreamService) {
        this.logStreamService = logStreamService;
    }

    public void runSimulation(JobEntity job, InputStream inputStream, long inputBytes) {
        final long startMs = System.currentTimeMillis();
        final AtomicLong seq = new AtomicLong(0);

        try {
            job.setInputBytes(inputBytes);

            set(job, JobStatus.UPLOADING, 10, "Input accepted");
            log(job, seq, "INFO", "UPLOADING", "Input accepted, bytes=" + inputBytes);

            set(job, JobStatus.MAPPING, 30, "Tokenizing text");
            log(job, seq, "INFO", "MAPPING", "Tokenization started");

            Map<String, Integer> counts = new HashMap<>(16_384);
            long tokenCount = 0L;
            long skippedCount = 0L;
            long lines = 0L;

            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines++;

                    String[] tokens = SPLIT.split(line);
                    for (String raw : tokens) {
                        String t = normalize(raw);
                        if (t.isBlank()) continue;
                        if (t.length() < minTokenLength) { skippedCount++; continue; }
                        if (enableStopwords && STOPWORDS.contains(t)) { skippedCount++; continue; }

                        counts.merge(t, 1, Integer::sum);
                        tokenCount++;
                    }

                    if (lines % 200 == 0) {
                        int progress = Math.min(70, 30 + (int) Math.min(40, lines / 200));
                        job.setProgress(progress);
                        log(job, seq, "INFO", "MAPPING",
                                "Processed lines=" + lines + ", tokens=" + tokenCount + ", unique=" + counts.size());
                    }
                }
            }

            job.setTokenCount(tokenCount);

            set(job, JobStatus.SHUFFLING, 78, "Grouping intermediate keys");
            log(job, seq, "INFO", "SHUFFLING", "Grouping keys, uniqueWords=" + counts.size());

            set(job, JobStatus.REDUCING, 90, "Aggregating frequencies");
            log(job, seq, "INFO", "REDUCING", "Sorting and selecting top=" + topN);

            // TOP N
            LinkedHashMap<String, Integer> top = counts.entrySet().stream()
                    .sorted((a, b) -> {
                        int c = Integer.compare(b.getValue(), a.getValue());
                        return c != 0 ? c : a.getKey().compareTo(b.getKey());
                    })
                    .limit(topN)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (a, b) -> a,
                            LinkedHashMap::new
                    ));

            // Agar xohlasangiz full resultni ham qaytaring:
            // job.setAllResult(counts); // modelda field bo'lsa
            job.setResult(top);

            set(job, JobStatus.DONE, 100, "Completed");
            long took = System.currentTimeMillis() - startMs;

            log(job, seq, "INFO", "DONE", "Completed in " + took + " ms");
            log(job, seq, "INFO", "DONE",
                    "Summary: tokens=" + tokenCount + ", skipped=" + skippedCount + ", unique=" + counts.size() + ", topN=" + topN);

        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            job.setProgress(100);
            job.setMessage("Failed");
            job.setError(e.getMessage());
            log(job, seq, "ERROR", "FAILED", "Job failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        } finally {
            logStreamService.completeJobStream(job.getId());
        }
    }

    private String normalize(String token) {
        if (token == null) return "";
        String t = token.trim().toLowerCase(Locale.ROOT);
        if (t.isEmpty()) return "";

        // Unicode normalize (accent removal)
        t = Normalizer.normalize(t, Normalizer.Form.NFKD);
        t = DIACRITICS.matcher(t).replaceAll("");

        return t;
    }

    private void set(JobEntity job, JobStatus status, int progress, String message) {
        job.setStatus(status);
        job.setProgress(progress);
        job.setMessage(message);
    }

    private void log(JobEntity job, AtomicLong seq, String level, String stage, String message) {
        logStreamService.publish(new LogEvent(
                job.getId(),
                level,
                stage,
                job.getProgress(),
                Instant.now().toEpochMilli(),
                "[" + seq.incrementAndGet() + "] " + message
        ));
    }
}