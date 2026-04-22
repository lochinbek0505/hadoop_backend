package uz.boqiyqahramonlar.hadoop_back_end.dto;

public record LogEvent(
        String jobId,
        String level,
        String stage,
        int progress,
        long ts,
        String message
) {}