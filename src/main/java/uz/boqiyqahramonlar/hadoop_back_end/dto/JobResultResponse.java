package uz.boqiyqahramonlar.hadoop_back_end.dto;

import java.util.Map;

public record JobResultResponse(String jobId, Map<String, Integer> topWords) {}