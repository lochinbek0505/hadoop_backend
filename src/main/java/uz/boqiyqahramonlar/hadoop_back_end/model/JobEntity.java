package uz.boqiyqahramonlar.hadoop_back_end.model;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class JobEntity {
    private final String id = UUID.randomUUID().toString();
    private JobStatus status = JobStatus.QUEUED;
    private int progress = 0;
    private String message = "Queued";
    private String error;
    private final Instant createdAt = Instant.now();
    private Instant updatedAt = Instant.now();
    private Map<String, Integer> result = new LinkedHashMap<>();
    private long inputBytes = 0L;
    private long tokenCount = 0L;

    public String getId() { return id; }
    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; touch(); }
    public int getProgress() { return progress; }
    public void setProgress(int progress) { this.progress = progress; touch(); }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; touch(); }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; touch(); }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public Map<String, Integer> getResult() { return result; }
    public void setResult(Map<String, Integer> result) { this.result = result; touch(); }
    public long getInputBytes() { return inputBytes; }
    public void setInputBytes(long inputBytes) { this.inputBytes = inputBytes; touch(); }
    public long getTokenCount() { return tokenCount; }
    public void setTokenCount(long tokenCount) { this.tokenCount = tokenCount; touch(); }

    private void touch() { this.updatedAt = Instant.now(); }
}