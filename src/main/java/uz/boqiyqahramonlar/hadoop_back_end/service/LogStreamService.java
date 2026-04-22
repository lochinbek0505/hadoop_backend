package uz.boqiyqahramonlar.hadoop_back_end.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import uz.boqiyqahramonlar.hadoop_back_end.dto.LogEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class LogStreamService {

    @Value("${app.sse-timeout-ms:1800000}")
    private long sseTimeoutMs;

    @Value("${app.max-log-lines-memory:500}")
    private int maxLogLines;

    private final Map<String, List<SseEmitter>> emittersByJob = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<LogEvent>> logBufferByJob = new ConcurrentHashMap<>();

    public SseEmitter subscribe(String jobId) {
        SseEmitter emitter = new SseEmitter(sseTimeoutMs);

        emittersByJob.computeIfAbsent(jobId, k -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeEmitter(jobId, emitter));
        emitter.onTimeout(() -> removeEmitter(jobId, emitter));
        emitter.onError((e) -> removeEmitter(jobId, emitter));

        // oldingi loglarni ham yuboramiz (UI reconnect bo'lsa)
        var history = logBufferByJob.getOrDefault(jobId, new CopyOnWriteArrayList<>());
        for (LogEvent event : history) {
            trySend(emitter, event);
        }

        return emitter;
    }

    public void publish(LogEvent event) {
        // memory buffer
        logBufferByJob.computeIfAbsent(event.jobId(), k -> new CopyOnWriteArrayList<>());
        var list = logBufferByJob.get(event.jobId());
        list.add(event);
        while (list.size() > maxLogLines) {
            list.remove(0);
        }

        // realtime emit
        var emitters = emittersByJob.getOrDefault(event.jobId(), List.of());
        for (SseEmitter emitter : emitters) {
            trySend(emitter, event);
        }
    }

    public void completeJobStream(String jobId) {
        var emitters = emittersByJob.getOrDefault(jobId, List.of());
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("done").data("completed"));
            } catch (IOException ignored) {}
            emitter.complete();
        }
        emittersByJob.remove(jobId);
    }

    private void trySend(SseEmitter emitter, LogEvent event) {
        try {
            emitter.send(SseEmitter.event()
                    .name("log")
                    .id(String.valueOf(event.ts()))
                    .data(event));
        } catch (IOException e) {
            emitter.completeWithError(e);
        }
    }

    private void removeEmitter(String jobId, SseEmitter emitter) {
        var list = emittersByJob.get(jobId);
        if (list != null) {
            list.remove(emitter);
            if (list.isEmpty()) emittersByJob.remove(jobId);
        }
    }
}