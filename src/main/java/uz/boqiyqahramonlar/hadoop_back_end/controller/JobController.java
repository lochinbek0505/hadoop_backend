package uz.boqiyqahramonlar.hadoop_back_end.controller;

import jakarta.validation.constraints.NotNull;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import uz.boqiyqahramonlar.hadoop_back_end.dto.CreateJobResponse;
import uz.boqiyqahramonlar.hadoop_back_end.dto.JobResultResponse;
import uz.boqiyqahramonlar.hadoop_back_end.dto.JobStatusResponse;
import uz.boqiyqahramonlar.hadoop_back_end.model.JobEntity;
import uz.boqiyqahramonlar.hadoop_back_end.service.JobService;
import uz.boqiyqahramonlar.hadoop_back_end.service.LogStreamService;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;
    private final LogStreamService logStreamService;

    public JobController(JobService jobService, LogStreamService logStreamService) {
        this.jobService = jobService;
        this.logStreamService = logStreamService;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<CreateJobResponse> create(@RequestParam("file") @NotNull MultipartFile file) {
        JobEntity job = jobService.create(file);
        return ResponseEntity.ok(new CreateJobResponse(job.getId(), job.getStatus().name()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobStatusResponse> status(@PathVariable String id) {
        JobEntity job = jobService.get(id);
        return ResponseEntity.ok(new JobStatusResponse(
                job.getId(),
                job.getStatus(),
                job.getProgress(),
                job.getMessage(),
                job.getInputBytes(),
                job.getTokenCount()
        ));
    }

    @GetMapping("/{id}/result")
    public ResponseEntity<JobResultResponse> result(@PathVariable String id) {
        JobEntity job = jobService.get(id);
        return ResponseEntity.ok(new JobResultResponse(job.getId(), job.getResult()));
    }

    @GetMapping(value = "/{id}/logs/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamLogs(@PathVariable String id) {
        // job borligini tekshirish
        jobService.get(id);
        return logStreamService.subscribe(id);
    }
}