package uz.boqiyqahramonlar.hadoop_back_end.repository;

import org.springframework.stereotype.Repository;
import uz.boqiyqahramonlar.hadoop_back_end.model.JobEntity;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class JobRepository {
    private final Map<String, JobEntity> jobs = new ConcurrentHashMap<>();

    public JobEntity save(JobEntity job) {
        jobs.put(job.getId(), job);
        return job;
    }

    public Optional<JobEntity> findById(String id) {
        return Optional.ofNullable(jobs.get(id));
    }
}