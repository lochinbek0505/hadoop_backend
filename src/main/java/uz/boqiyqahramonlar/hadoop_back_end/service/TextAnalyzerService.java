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
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class TextAnalyzerService {

    @Value("${app.top-n:20}")
    private int topN;

    private final LogStreamService logStreamService;

    private static final Pattern SPLIT = Pattern.compile("[^\\p{L}\\p{Nd}]+");

    public TextAnalyzerService(LogStreamService logStreamService) {
        this.logStreamService = logStreamService;
    }

    public void runSimulation(JobEntity job, InputStream inputStream, long inputBytes) {
        long start = System.currentTimeMillis();
        try {
            job.setInputBytes(inputBytes);

            // STAGE 1
            set(job, JobStatus.UPLOADING, 10, "Input accepted");
            log(job, "INFO", "UPLOADING", "INFO mapreduce.JobSubmitter: number of splits: 4");
            sleep(200);

            // STAGE 2
            set(job, JobStatus.MAPPING, 35, "Tokenizing text");
            log(job, "INFO", "MAPPING", "INFO mapreduce.Job: map 15% reduce 0%");

            Map<String, Integer> counts = new HashMap<>();
            long tokenCount = 0L;
            long lines = 0L;

            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines++;
                    String[] tokens = SPLIT.split(line.toLowerCase(Locale.ROOT));
                    for (String t : tokens) {
                        if (!t.isBlank()) {
                            counts.merge(t, 1, Integer::sum);
                            tokenCount++;
                        }
                    }

                    if (lines % 500 == 0) {
                        int p = Math.min(65, 35 + (int)(lines % 30));
                        job.setProgress(p);
                        log(job, "INFO", "MAPPING", "INFO mapreduce.Job: map " + p + "% reduce 0%");
                    }
                }
            }

            job.setTokenCount(tokenCount);
            set(job, JobStatus.SHUFFLING, 72, "Grouping intermediate keys");
            log(job, "INFO", "SHUFFLING", "INFO mapreduce.Shuffle: copy(4 of 4) completed");
            sleep(150);

            set(job, JobStatus.REDUCING, 88, "Aggregating frequencies");
            log(job, "INFO", "REDUCING", "INFO mapreduce.Job: map 100% reduce 80%");

            Map<String, Integer> top = counts.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .limit(topN)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (a, b) -> a,
                            LinkedHashMap::new
                    ));

            job.setResult(top);
            set(job, JobStatus.DONE, 100, "Completed");
            long took = System.currentTimeMillis() - start;
            log(job, "INFO", "DONE", "INFO mapreduce.Job: Job completed successfully in " + took + " ms");
            log(job, "INFO", "DONE", "Counters: tokens=" + tokenCount + ", uniqueWords=" + counts.size());

        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            job.setProgress(100);
            job.setMessage("Failed");
            job.setError(e.getMessage());
            log(job, "ERROR", "FAILED", "ERROR mapreduce.Job: " + e.getMessage());
        } finally {
            logStreamService.completeJobStream(job.getId());
        }
    }

    private void set(JobEntity job, JobStatus status, int progress, String message) {
        job.setStatus(status);
        job.setProgress(progress);
        job.setMessage(message);
    }

    private void log(JobEntity job, String level, String stage, String message) {
        logStreamService.publish(new LogEvent(
                job.getId(),
                level,
                stage,
                job.getProgress(),
                Instant.now().toEpochMilli(),
                message
        ));
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}