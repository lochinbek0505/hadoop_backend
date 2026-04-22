package uz.boqiyqahramonlar.hadoop_back_end.model;

public enum JobStatus {
    QUEUED,
    UPLOADING,
    MAPPING,
    SHUFFLING,
    REDUCING,
    DONE,
    FAILED
}