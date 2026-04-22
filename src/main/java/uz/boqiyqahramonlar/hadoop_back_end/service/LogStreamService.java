package uz.boqiyqahramonlar.hadoop_back_end.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import uz.boqiyqahramonlar.hadoop_back_end.dto.LogEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class LogStreamService {

    @Value("${app.sse-timeout-ms:1800000}")
    private long sseTimeoutMs;

    @Value("${app.max-log-lines-memory:1000}")
    private int maxLogLines;

    @Value("${app.sse-heartbeat-ms:15000}")
    private long heartbeatMs;

    private final Map<String, CopyOnWriteArrayList<SseEmitter>> emittersByJob = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedDeque<LogEvent>> logBufferByJob = new ConcurrentHashMap<>();

    private final ScheduledExecutorService heartbeatPool =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "sse-heartbeat");
                t.setDaemon(true);
                return t;
            });

    public LogStreamService() {
        heartbeatPool.scheduleAtFixedRate(this::sendHeartbeats, 10, 15, TimeUnit.SECONDS);
    }

    public SseEmitter subscribe(String jobId) {
        SseEmitter emitter = new SseEmitter(sseTimeoutMs);

        emittersByJob.computeIfAbsent(jobId, k -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeEmitter(jobId, emitter));
        emitter.onTimeout(() -> removeEmitter(jobId, emitter));
        emitter.onError((e) -> removeEmitter(jobId, emitter));

        // reconnect bo'lsa tarixdan yuborish
        var history = logBufferByJob.getOrDefault(jobId, new ConcurrentLinkedDeque<>());
        for (LogEvent event : history) {
            trySend(emitter, "log", String.valueOf(event.ts()), event);
        }

        // connected event
        trySend(emitter, "connected", String.valueOf(System.currentTimeMillis()), "subscribed");

        return emitter;
    }

    public void publish(LogEvent event) {
        // bounded memory buffer
        var deque = logBufferByJob.computeIfAbsent(event.jobId(), k -> new ConcurrentLinkedDeque<>());
        deque.addLast(event);
        while (deque.size() > maxLogLines) {
            deque.pollFirst();
        }

        // realtime emit
        var emitters = emittersByJob.getOrDefault(event.jobId(), new CopyOnWriteArrayList<>());
        for (SseEmitter emitter : emitters) {
            trySend(emitter, "log", String.valueOf(event.ts()), event);
        }
    }

    public void completeJobStream(String jobId) {
        var emitters = emittersByJob.getOrDefault(jobId, new CopyOnWriteArrayList<>());
        for (SseEmitter emitter : emitters) {
            trySend(emitter, "done", String.valueOf(System.currentTimeMillis()), "completed");
            emitter.complete();
        }
        emittersByJob.remove(jobId);
        // log bufferni biroz vaqt saqlab turishni ham qilsa bo'ladi, hozir qoldiryapmiz
    }

    private void trySend(SseEmitter emitter, String eventName, String id, Object data) {
        try {
            emitter.send(SseEmitter.event().name(eventName).id(id).data(data));
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

    private void sendHeartbeats() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, CopyOnWriteArrayList<SseEmitter>> entry : emittersByJob.entrySet()) {
            for (SseEmitter emitter : entry.getValue()) {
                trySend(emitter, "ping", String.valueOf(now), "keep-alive");
            }
        }
    }
}