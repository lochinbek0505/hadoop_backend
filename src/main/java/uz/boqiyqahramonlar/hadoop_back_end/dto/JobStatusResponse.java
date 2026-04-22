package uz.boqiyqahramonlar.hadoop_back_end.dto;


import uz.boqiyqahramonlar.hadoop_back_end.model.JobStatus;

public record JobStatusResponse(
        String jobId,
        JobStatus status,
        int progress,
        String message,
        long inputBytes,
        long tokenCount
) {}