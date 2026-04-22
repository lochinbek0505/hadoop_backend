package uz.boqiyqahramonlar.hadoop_back_end.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ExecutorConfig {
    @Bean(destroyMethod = "shutdown")
    public ExecutorService jobExecutor() {
        // 1 yadro server uchun
        return Executors.newSingleThreadExecutor();
    }
}