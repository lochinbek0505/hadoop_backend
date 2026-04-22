package uz.boqiyqahramonlar.hadoop_back_end.service;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import uz.boqiyqahramonlar.hadoop_back_end.model.JobEntity;
import uz.boqiyqahramonlar.hadoop_back_end.model.JobStatus;
import uz.boqiyqahramonlar.hadoop_back_end.repository.JobRepository;

import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final TextAnalyzerService textAnalyzerService;
    private final ExecutorService executorService;

    public JobService(JobRepository jobRepository,
                      TextAnalyzerService textAnalyzerService,
                      ExecutorService executorService) {
        this.jobRepository = jobRepository;
        this.textAnalyzerService = textAnalyzerService;
        this.executorService = executorService;
    }

    public JobEntity create(MultipartFile file) {
        JobEntity job = new JobEntity();
        jobRepository.save(job);

        executorService.submit(() -> {
            try (InputStream in = file.getInputStream()) {
                textAnalyzerService.runSimulation(job, in, file.getSize());
            } catch (Exception e) {
                job.setStatus(JobStatus.FAILED);
                job.setMessage("Cannot read input file");
                job.setError(e.getMessage());
            }
        });

        return job;
    }

    public uz.boqiyqahramonlar.hadoop_back_end.model.JobEntity get(String id) {
        return jobRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Job not found: " + id));
    }
}